import type { MessageStore } from '../index.js';
import type { SignatureInput } from '../types/jws-types.js';
import type { RecordsDeleteDescriptor, RecordsDeleteMessage } from '../types/records-types.js';

import { getCurrentTimeInHighPrecision } from '../utils/time.js';
import { Message } from '../core/message.js';
import { RecordsGrantAuthorization } from '../core/records-grant-authorization.js';
import { validateAuthorizationIntegrity } from '../core/auth.js';
import { DwnInterfaceName, DwnMethodName } from '../core/message.js';

export type RecordsDeleteOptions = {
  recordId: string;
  messageTimestamp?: string;
  authorizationSignatureInput: SignatureInput;
  permissionsGrantId?: string;
};

export class RecordsDelete extends Message<RecordsDeleteMessage> {

  public static async parse(message: RecordsDeleteMessage): Promise<RecordsDelete> {
    await validateAuthorizationIntegrity(message);

    const recordsDelete = new RecordsDelete(message);
    return recordsDelete;
  }

  /**
   * Creates a RecordsDelete message.
   * @param options.recordId If `undefined`, will be auto-filled as a originating message as convenience for developer.
   * @param options.messageTimestamp If `undefined`, it will be auto-filled with current time.
   */
  public static async create(options: RecordsDeleteOptions): Promise<RecordsDelete> {
    const recordId = options.recordId;
    const currentTime = getCurrentTimeInHighPrecision();

    const descriptor: RecordsDeleteDescriptor = {
      interface        : DwnInterfaceName.Records,
      method           : DwnMethodName.Delete,
      recordId,
      messageTimestamp : options.messageTimestamp ?? currentTime
    };

    const authorization = await Message.signAsAuthorization(descriptor, options.authorizationSignatureInput, options.permissionsGrantId);
    const message: RecordsDeleteMessage = { descriptor, authorization };

    Message.validateJsonSchema(message);

    return new RecordsDelete(message);
  }

  public async authorize(tenant: string, messageStore: MessageStore): Promise<void> {
    // TODO: #203 - implement protocol-based authorization for RecordsDelete (https://github.com/TBD54566975/dwn-sdk-js/issues/203)

    if (tenant === this.author) {
      // if author is the same as the target tenant, we can directly grant access
    } else if (this.authorizationPayload?.permissionsGrantId) {
      await RecordsGrantAuthorization.authorizeRecordsGrant(tenant, this, messageStore);
    } else {
      throw new Error('message failed authorization');
    }
  }
}
