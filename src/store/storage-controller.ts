import type { DataStore } from '../types/data-store.js';
import type { EventLog } from '../types/event-log.js';
import type { MessageStore } from '../types/message-store.js';
import type { Filter, GenericMessage, TimestampedMessage } from '../types/message-types.js';
import type { RecordsDeleteMessage, RecordsWriteMessage } from '../types/records-types.js';

import { constructRecordsWriteIndexes } from '../handlers/records-write.js';
import { DwnConstant } from '../core/dwn-constant.js';
import { lexicographicalCompare } from '../utils/string.js';
import { Records } from '../index.js';
import { RecordsWrite } from '../interfaces/records-write.js';
import { DwnInterfaceName, DwnMethodName, Message } from '../core/message.js';

/**
 * A class that provides an abstraction for the usage of MessageStore, DataStore, and EventLog.
 */
export class StorageController {
  /**
   * Deletes a message.
   */
  private static async delete(
    messageStore: MessageStore,
    dataStore: DataStore,
    tenant: string,
    message: GenericMessage
  ): Promise<void> {
    const messageCid = await Message.getCid(message);

    if (message.descriptor.method === DwnMethodName.Write &&
        (message as RecordsWriteMessage).descriptor.dataSize > DwnConstant.maxDataSizeAllowedToBeEncoded) {
      const recordsWriteMessage = message as RecordsWriteMessage;
      await dataStore.delete(tenant, messageCid, recordsWriteMessage.descriptor.dataCid);
    }

    await messageStore.delete(tenant, messageCid);
  }


  public static async purgeDescendants(
    tenant: string,
    parentRecordId: string,
    messageStore: MessageStore,
    dataStore: DataStore,
    eventLog: EventLog
  ): Promise<void> {
    const deletedMessageCids: string[] = [];
    const recordIds: string[] = [];
    const messages = await messageStore.query(tenant, { parentId: parentRecordId });
    for (const message of messages) {
      await StorageController.delete(messageStore, dataStore, tenant, message);
      const messageCid = await Message.getCid(message);
      const recordId = Message.getRecordId(message);
      deletedMessageCids.push(messageCid);
      if (recordId) {
        recordIds.push(recordId);
      }
    }
    await eventLog.deleteEventsByCid(tenant, deletedMessageCids);
    await Promise.all(recordIds.map(recordId => this.purgeDescendants(tenant, recordId, messageStore, dataStore, eventLog)));
  }


  /**
   * Deletes all messages in `existingMessages` that are older than the `comparedToMessage` in the given tenant,
   * but keep the initial write write for future processing by ensuring its `isLatestBaseState` index is "false".
   */
  public static async deleteAllOlderMessagesButKeepInitialWrite(
    tenant: string,
    existingMessages: TimestampedMessage[],
    comparedToMessage: TimestampedMessage,
    messageStore: MessageStore,
    dataStore: DataStore,
    eventLog: EventLog
  ): Promise<void> {
    const deletedMessageCids: string[] = [];

    // NOTE: under normal operation, there should only be at most two existing records per `recordId` (initial + a potential subsequent write/delete),
    // but the DWN may crash before `delete()` is called below, so we use a loop as a tactic to clean up lingering data as needed
    for (const message of existingMessages) {
      const messageIsOld = await Message.isOlder(message, comparedToMessage);
      if (messageIsOld) {
      // the easiest implementation here is delete each old messages
      // and re-create it with the right index (isLatestBaseState = 'false') if the message is the initial write,
      // but there is room for better/more efficient implementation here
        await StorageController.delete(messageStore, dataStore, tenant, message);

        // if the existing message is the initial write
        // we actually need to keep it BUT, need to ensure the message is no longer marked as the latest state
        const existingMessageIsInitialWrite = await RecordsWrite.isInitialWrite(message);
        if (existingMessageIsInitialWrite) {
          const existingRecordsWrite = await RecordsWrite.parse(message as RecordsWriteMessage);
          const isLatestBaseState = false;
          const indexes = await constructRecordsWriteIndexes(existingRecordsWrite, isLatestBaseState);
          const writeMessage = message as RecordsWriteMessageWithOptionalEncodedData;
          delete writeMessage.encodedData;
          await messageStore.put(tenant, writeMessage, indexes);
        } else {
          const messageCid = await Message.getCid(message);
          deletedMessageCids.push(messageCid);
        }
      }

      await eventLog.deleteEventsByCid(tenant, deletedMessageCids);
    }
  }

  public static async deletePathLimitMessages(
    tenant: string,
    pathKeepLimit: number,
    incomingMessage: RecordsWrite,
    pathMessages: (RecordsWriteMessage|RecordsDeleteMessage)[],
    messageStore: MessageStore,
    dataStore: DataStore,
    eventLog: EventLog
  ): Promise<void> {
    // get records to purge, sort by most recent first, get unique record Ids.
    const recordIdsToPurge = pathMessages.filter( message => Records.getRecordId(message) !== incomingMessage.message.recordId)
      .sort((a, b) => lexicographicalCompare(b.descriptor.messageTimestamp, a.descriptor.messageTimestamp))
      .reduce((acc: string[], current: TimestampedMessage): string[] => {
        const recordId = Records.getRecordId(current as (RecordsWriteMessage | RecordsDeleteMessage));
        if (acc.includes(recordId)) {
          return acc;
        }
        return [ ...acc, recordId ];
      }, []).filter((_,i) => i >= (pathKeepLimit - 1));

    for (const recordId of recordIdsToPurge) {
      // get records that could break without the parent.
      const parentQuery: Filter = {
        interface : DwnInterfaceName.Records,
        parentId  : recordId,
      };
      const purgeParents = await messageStore.query(tenant, parentQuery) as (RecordsWriteMessage | RecordsDeleteMessage)[];
      // get records that could break without the context.
      const contextQuery: Filter = {
        interface : DwnInterfaceName.Records,
        contextId : recordId,
      };
      const purgeContext = await messageStore.query(tenant, contextQuery) as (RecordsWriteMessage | RecordsDeleteMessage)[];
      const combinedRecords = [...purgeParents, ...purgeContext];
      combinedRecords.forEach(r => {
        const recordId = Records.getRecordId(r);
        if (!recordIdsToPurge.includes(recordId)) {
          recordIdsToPurge.push(recordId);
        }
      });
      pathMessages.push(...combinedRecords);
    }

    const deletedMessageCids: string[] = [];
    for (const message of pathMessages.filter(m => recordIdsToPurge.includes(Records.getRecordId(m)))) {
      const messageCid = await Message.getCid(message);
      await StorageController.delete(messageStore, dataStore, tenant, message);
      deletedMessageCids.push(messageCid);
      const recordId = Message.getRecordId(message);
      if (recordId) {
        await this.purgeDescendants(tenant, recordId, messageStore, dataStore, eventLog);
      }
    }
    await eventLog.deleteEventsByCid(tenant, deletedMessageCids);
  }
}

// records with a data size below a threshold are stored within MessageStore with their data embedded
export type RecordsWriteMessageWithOptionalEncodedData = RecordsWriteMessage & { encodedData?: string };
