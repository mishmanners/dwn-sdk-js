import type { GenericMessage } from '../types/message-types.js';
import type { MessageStore } from '../types/message-store.js';
import type { PermissionsGrantMessage } from '../index.js';
import type { PermissionsRecordReadScope } from '../types/permissions-types.js';
import type { RecordsWrite } from './records-write.js';
import type { SignatureInput } from '../types/jws-types.js';
import type { RecordsReadDescriptor, RecordsReadMessage } from '../types/records-types.js';

import { getCurrentTimeInHighPrecision } from '../utils/time.js';
import { GrantAuthorization } from '../core/grant-authorization.js';
import { Message } from '../core/message.js';
import { ProtocolAuthorization } from '../core/protocol-authorization.js';
import { validateAuthorizationIntegrity } from '../core/auth.js';
import { DwnError, DwnErrorCode } from '../index.js';
import { DwnInterfaceName, DwnMethodName } from '../core/message.js';

export type RecordsReadOptions = {
  recordId: string;
  date?: string;
  authorizationSignatureInput?: SignatureInput;
};

export class RecordsRead extends Message<RecordsReadMessage> {

  public static async parse(message: RecordsReadMessage): Promise<RecordsRead> {
    if (message.authorization !== undefined) {
      await validateAuthorizationIntegrity(message as GenericMessage);
    }

    const recordsRead = new RecordsRead(message);
    return recordsRead;
  }

  /**
   * Creates a RecordsRead message.
   * @param options.recordId If `undefined`, will be auto-filled as a originating message as convenience for developer.
   * @param options.date If `undefined`, it will be auto-filled with current time.
   */
  public static async create(options: RecordsReadOptions): Promise<RecordsRead> {
    const { recordId, authorizationSignatureInput } = options;
    const currentTime = getCurrentTimeInHighPrecision();

    const descriptor: RecordsReadDescriptor = {
      interface        : DwnInterfaceName.Records,
      method           : DwnMethodName.Read,
      recordId,
      messageTimestamp : options.date ?? currentTime
    };

    // only generate the `authorization` property if signature input is given
    const authorization = authorizationSignatureInput ? await Message.signAsAuthorization(descriptor, authorizationSignatureInput) : undefined;
    const message: RecordsReadMessage = { descriptor, authorization };

    Message.validateJsonSchema(message);

    return new RecordsRead(message);
  }

  public async authorize(tenant: string, newestRecordsWrite: RecordsWrite, messageStore: MessageStore): Promise<void> {
    const { descriptor } = newestRecordsWrite.message;
    // if author is the same as the target tenant, we can directly grant access
    if (this.author === tenant) {
      return;
    } else if (descriptor.published === true) {
      // authentication is not required for published data
      return;
    } else if (this.author !== undefined && this.author === descriptor.recipient) {
      // The recipient of a message may always read it
      return;
    } else if (this.author !== undefined && this.authorizationPayload?.permissionsGrantId) {
      const permissionsGrantMessage = await GrantAuthorization.authorizeGenericMessage(tenant, this, this.author, messageStore);
      this.validateGrantScope(permissionsGrantMessage);
    } else if (descriptor.protocol !== undefined) {
      await ProtocolAuthorization.authorize(tenant, this, this.author, messageStore);
    } else {
      throw new Error('message failed authorization');
    }
  }

  private async validateGrantScope(permissionsGrantMessage: PermissionsGrantMessage): Promise<void> {
    const grantScope = permissionsGrantMessage.descriptor.scope as PermissionsRecordReadScope;
    const grantedRecordIds = grantScope.recordIds;
    const recordId = this.message.descriptor.recordId;
    if (!grantedRecordIds.includes(recordId)) {
      // attempting to read a record that is not within the grant scope
      throw new DwnError(
        DwnErrorCode.RecordsReadRecordIdOutOfGrantScope,
        `RecordId ${recordId}`
      );
    }
  }
}
