import type { GeneralJws } from '../types/jws-types.js';
import type { SignatureInput } from '../types/jws-types.js';
import type { BaseAuthorizationPayload, Descriptor, GenericMessage, TimestampedMessage } from '../types/message-types.js';

import { Cid } from '../utils/cid.js';
import { GeneralJwsSigner } from '../jose/jws/general/signer.js';
import { Jws } from '../utils/jws.js';
import { lexicographicalCompare } from '../utils/string.js';
import { removeUndefinedProperties } from '../utils/object.js';
import { validateJsonSchema } from '../schema-validator.js';
import { DwnError, DwnErrorCode, EventsGet, EventsGetMessage, EventsGetOptions, MessagesGet, MessagesGetMessage, MessagesGetOptions, PermissionsGrant, PermissionsGrantDescriptor, PermissionsGrantMessage, PermissionsGrantOptions, PermissionsRequest, PermissionsRequestDescriptor, PermissionsRequestMessage, PermissionsRequestOptions, PermissionsRevoke, PermissionsRevokeDescriptor, PermissionsRevokeMessage, PermissionsRevokeOptions, ProtocolsConfigure, ProtocolsConfigureDescriptor, ProtocolsConfigureMessage, ProtocolsConfigureOptions, ProtocolsQuery, ProtocolsQueryMessage, ProtocolsQueryOptions, RecordsDelete, RecordsDeleteMessage, RecordsDeleteOptions, RecordsQuery, RecordsQueryMessage, RecordsQueryOptions, RecordsRead, RecordsReadOptions, RecordsWrite, RecordsWriteMessage, RecordsWriteOptions } from '../index.js';
import { RecordsDeleteDescriptor, RecordsQueryDescriptor, RecordsReadDescriptor, RecordsReadMessage, RecordsWriteDescriptor } from '../types/records-types.js';
import { EventsGetDescriptor } from '../types/event-types.js';
import { MessagesGetDescriptor } from '../types/messages-types.js';
import { ProtocolsQueryDescriptor } from '../types/protocols-types.js';

export enum DwnInterfaceName {
  Events = 'Events',
  Hooks = 'Hooks',
  Messages = 'Messages',
  Permissions = 'Permissions',
  Protocols = 'Protocols',
  Records = 'Records',
  Snapshots = 'Snapshots'
}

export enum DwnMethodName {
  Configure = 'Configure',
  Create = 'Create',
  Get = 'Get',
  Grant = 'Grant',
  Query = 'Query',
  Read = 'Read',
  Request = 'Request',
  Revoke = 'Revoke',
  Write = 'Write',
  Delete = 'Delete'
}

type DwnMessageClassIntersection =
  EventsGet |
  MessagesGet |
  PermissionsGrant |
  PermissionsRequest |
  PermissionsRevoke |
  ProtocolsConfigure |
  ProtocolsQuery |
  RecordsDelete |
  RecordsQuery |
  RecordsRead |
  RecordsWrite;

type DwnMessageIntersection =
  EventsGetMessage |
  MessagesGetMessage |
  PermissionsGrantMessage |
  PermissionsRequestMessage |
  PermissionsRevokeMessage |
  ProtocolsConfigureMessage |
  ProtocolsQueryMessage |
  RecordsDeleteMessage |
  RecordsQueryMessage |
  RecordsReadMessage |
  RecordsWriteMessage;

enum DwnMessageType {
  EventsGet = 'EventsGet',
  MessagesGet = 'MessagesGet',
  PermissionsGrant = 'PermissionsGrant',
  PermissionsRequest = 'PermissionsRequest',
  PermissionsRevoke = 'PermissionsRevoke',
  ProtocolsConfigure = 'ProtocolsConfigure',
  ProtocolsQuery = 'ProtocolsQuery',
  RecordsDelete = 'RecordsDelete',
  RecordsQuery = 'RecordsQuery',
  RecordsRead = 'RecordsRead',
  RecordsWrite = 'RecordsWrite'
}

type DwnMessageUnsignedIntersection =
  EventsGetDescriptor |
  MessagesGetDescriptor |
  PermissionsGrantDescriptor |
  PermissionsRequestDescriptor |
  PermissionsRevokeDescriptor |
  ProtocolsConfigureDescriptor |
  ProtocolsQueryDescriptor |
  RecordsDeleteDescriptor |
  RecordsQueryDescriptor |
  RecordsReadDescriptor |
  RecordsWriteDescriptor;

/**
 * Generic type for valid options to pass to Message.create() for specific DWN message types
 */
export type GenericMessageCreateOptions<M> =
  M extends ProtocolsConfigureMessage ? ProtocolsConfigureOptions :
  M extends ProtocolsQueryMessage ? ProtocolsQueryOptions :
  M extends RecordsDeleteMessage ? RecordsDeleteOptions :
  M extends RecordsQueryMessage ? RecordsQueryOptions :
  M extends RecordsReadMessage ? RecordsReadOptions :
  M extends RecordsWriteMessage ? RecordsWriteOptions :
  M extends PermissionsGrantMessage ? PermissionsGrantOptions :
  M extends PermissionsRequestMessage ? PermissionsRequestOptions :
  M extends PermissionsRevokeMessage ? PermissionsRevokeOptions :
  never;

type MessageCreateOptionsIntersection =
  EventsGetOptions |
  MessagesGetOptions |
  PermissionsGrantOptions |
  PermissionsRequestOptions |
  PermissionsRevokeOptions |
  ProtocolsConfigureOptions |
  ProtocolsQueryOptions |
  RecordsDeleteOptions |
  RecordsQueryOptions |
  RecordsReadOptions |
  RecordsWriteOptions;

export class MessageBuilder {

  static parse(message: EventsGetMessage): Promise<EventsGet>;
  static parse(message: MessagesGetMessage): Promise<MessagesGet>;
  static parse(message: PermissionsGrantMessage): Promise<PermissionsGrant>;
  static parse(message: PermissionsRequestMessage): Promise<PermissionsRequest>;
  static parse(message: PermissionsRevokeMessage): Promise<PermissionsRevoke>;
  static parse(message: ProtocolsConfigureMessage): Promise<ProtocolsConfigure>;
  static parse(message: ProtocolsQueryMessage): Promise<ProtocolsQuery>;
  static parse(message: RecordsDeleteMessage): Promise<RecordsDelete>;
  static parse(message: RecordsQueryMessage): Promise<RecordsQuery>;
  static parse(message: RecordsReadMessage): Promise<RecordsRead>;
  static parse(message: RecordsWriteMessage): Promise<RecordsWrite>;
  /**
   * Parse, validate, and wrap a raw DWN Message.
   * @param message Raw DWN Message
   * @returns DWN Message class which wraps the validated DWN message.
   * @throws {Error} If message type could not be ascertained or if parsing otherwise fails.
   */
  static parse<T extends DwnMessageIntersection>(message: T): Promise<DwnMessageClassIntersection> {
    const dwnMessageType = message.descriptor.interface + message.descriptor.method;

    switch (dwnMessageType) {
    case 'EventsGet': return EventsGet.parse(message as EventsGetMessage);
    case 'MessagesGet': return MessagesGet.parse(message as MessagesGetMessage);
    case 'PermissionsGrant': return PermissionsGrant.parse(message as PermissionsGrantMessage);
    case 'PermissionsRequest': return PermissionsRequest.parse(message as PermissionsRequestMessage);
    case 'PermissionsRevoke': return PermissionsRevoke.parse(message as PermissionsRevokeMessage);
    case 'ProtocolsConfigure': return ProtocolsConfigure.parse(message as ProtocolsConfigureMessage);
    case 'ProtocolsQuery': return ProtocolsQuery.parse(message as ProtocolsQueryMessage);
    case 'RecordsDelete': return RecordsDelete.parse(message as RecordsDeleteMessage);
    case 'RecordsQuery': return RecordsQuery.parse(message as RecordsQueryMessage);
    case 'RecordsRead': return RecordsRead.parse(message as RecordsReadMessage);
    case 'RecordsWrite': return RecordsWrite.parse(message as RecordsWriteMessage);

    default:
      throw new DwnError(DwnErrorCode.MessageParseTypeNotRecognized, `Could not create message of type ${dwnMessageType}`);
    }
  }


  static create(messageType: string, options: EventsGetOptions): Promise<EventsGet>;
  static create(messageType: string, options: MessagesGetOptions): Promise<MessagesGet>;
  static create(messageType: string, options: PermissionsGrantOptions): Promise<PermissionsGrant>;
  static create(messageType: string, options: PermissionsRequestOptions): Promise<PermissionsRequest>;
  static create(messageType: string, options: PermissionsRevokeOptions): Promise<PermissionsRevoke>;
  static create(messageType: string, options: ProtocolsConfigureOptions): Promise<ProtocolsConfigure>;
  static create(messageType: string, options: ProtocolsQueryOptions): Promise<ProtocolsQuery>;
  static create(messageType: string, options: RecordsDeleteOptions): Promise<RecordsDelete>;
  static create(messageType: string, options: RecordsQueryOptions): Promise<RecordsQuery>;
  static create(messageType: string, options: RecordsReadOptions): Promise<RecordsRead>;
  static create(messageType: string, options: RecordsWriteOptions): Promise<RecordsWrite>;
  /**
   * Generic method for creating and validating a signed DWN message.
   * @param messageType Name of the DWN Message being created, e.g. 'RecordsWrite', 'ProtocolsConfigure;
   * @param options Message-specific creation options
   * @returns DWN Message class which wraps the raw DWN message
   */
  static create(messageType: DwnMessageType, options: MessageCreateOptionsIntersection)
    : Promise<DwnMessageClassIntersection> {

    switch (messageType) {
    case 'EventsGet': return EventsGet.create(options as EventsGetOptions);
    case 'MessagesGet': return MessagesGet.create(options as MessagesGetOptions);
    case 'PermissionsGrant': return PermissionsGrant.create(options as PermissionsGrantOptions);
    case 'PermissionsRequest': return PermissionsRequest.create(options as PermissionsRequestOptions);
    case 'PermissionsRevoke': return PermissionsRevoke.create(options as PermissionsRevokeOptions);
    case 'ProtocolsConfigure': return ProtocolsConfigure.create(options as ProtocolsConfigureOptions);
    case 'ProtocolsQuery': return ProtocolsQuery.create(options as ProtocolsQueryOptions);
    case 'RecordsDelete': return RecordsDelete.create(options as RecordsDeleteOptions);
    case 'RecordsQuery': return RecordsQuery.create(options as RecordsQueryOptions);
    case 'RecordsRead': return RecordsRead.create(options as RecordsReadOptions);
    case 'RecordsWrite': return RecordsWrite.create(options as RecordsWriteOptions);

    default:
      throw new DwnError(DwnErrorCode.MessageCreateSignedTypeNotRecognized, `Could not create message of type ${messageType}`);
    }
  }
}

export abstract class Message<M extends GenericMessage> {
  readonly message: M;
  readonly authorizationPayload: BaseAuthorizationPayload | undefined;
  readonly author: string | undefined;

  constructor(message: M) {
    this.message = message;

    if (message.authorization !== undefined) {
      this.authorizationPayload = Jws.decodePlainObjectPayload(message.authorization);
      this.author = Message.getAuthor(message as GenericMessage);
    }
  }

  /**
   * Called by `JSON.stringify(...)` automatically.
   */
  toJSON(): GenericMessage {
    return this.message;
  }

  /**
   * Validates the given message against the corresponding JSON schema.
   * @throws {Error} if fails validation.
   */
  public static validateJsonSchema(rawMessage: any): void {
    const dwnInterface = rawMessage.descriptor.interface;
    const dwnMethod = rawMessage.descriptor.method;
    const schemaLookupKey = dwnInterface + dwnMethod;

    // throws an error if message is invalid
    validateJsonSchema(schemaLookupKey, rawMessage);
  };

  /**
   * Gets the DID of the author of the given message, returned `undefined` if message is not signed.
   */
  public static getAuthor(message: GenericMessage): string | undefined {
    if (message.authorization === undefined) {
      return undefined;
    }

    const author = Jws.getSignerDid(message.authorization.signatures[0]);
    return author;
  }

  /**
   * Gets the CID of the given message.
   */
  public static async getCid(message: GenericMessage): Promise<string> {
    // NOTE: we wrap the `computeCid()` here in case that
    // the message will contain properties that should not be part of the CID computation
    // and we need to strip them out (like `encodedData` that we historically had for a long time),
    // but we can remove this method entirely if the code becomes stable and it is apparent that the wrapper is not needed

    // ^--- seems like we might need to keep this around for now.
    const rawMessage = { ...message } as any;
    if (rawMessage.encodedData) {
      delete rawMessage.encodedData;
    }

    const cid = await Cid.computeCid(rawMessage as GenericMessage);
    return cid;
  }

  /**
   * Compares message CID in lexicographical order according to the spec.
   * @returns 1 if `a` is larger than `b`; -1 if `a` is smaller/older than `b`; 0 otherwise (same message)
   */
  public static async compareCid(a: GenericMessage, b: GenericMessage): Promise<number> {
    // the < and > operators compare strings in lexicographical order
    const cidA = await Message.getCid(a);
    const cidB = await Message.getCid(b);
    return lexicographicalCompare(cidA, cidB);
  }

  /**
   * Compares the CID of two messages.
   * @returns `true` if `a` is newer than `b`; `false` otherwise
   */
  public static async isCidLarger(a: GenericMessage, b: GenericMessage): Promise<boolean> {
    const aIsLarger = (await Message.compareCid(a, b) > 0);
    return aIsLarger;
  }

  /**
   * @returns message with the largest CID in the array using lexicographical compare. `undefined` if given array is empty.
   */
  public static async getMessageWithLargestCid(messages: GenericMessage[]): Promise<GenericMessage | undefined> {
    let currentNewestMessage: GenericMessage | undefined = undefined;
    for (const message of messages) {
      if (currentNewestMessage === undefined || await Message.isCidLarger(message, currentNewestMessage)) {
        currentNewestMessage = message;
      }
    }

    return currentNewestMessage;
  }

  /**
   * Signs over the CID of provided `descriptor`. The output is used as an `authorization` property.
   * @param signatureInput - the signature material to use (e.g. key and header data)
   * @returns General JWS signature used as an `authorization` property.
   */
  public static async signAsAuthorization(
    descriptor: Descriptor,
    signatureInput: SignatureInput,
    permissionsGrantId?: string,
  ): Promise<GeneralJws> {
    const descriptorCid = await Cid.computeCid(descriptor);

    const authPayload: BaseAuthorizationPayload = { descriptorCid, permissionsGrantId };
    removeUndefinedProperties(authPayload);
    const authPayloadStr = JSON.stringify(authPayload);
    const authPayloadBytes = new TextEncoder().encode(authPayloadStr);

    const signer = await GeneralJwsSigner.create(authPayloadBytes, [signatureInput]);

    return signer.getJws();
  }

  /**
   * @returns newest message in the array. `undefined` if given array is empty.
   */
  public static async getNewestMessage(messages: TimestampedMessage[]): Promise<TimestampedMessage | undefined> {
    let currentNewestMessage: TimestampedMessage | undefined = undefined;
    for (const message of messages) {
      if (currentNewestMessage === undefined || await Message.isNewer(message, currentNewestMessage)) {
        currentNewestMessage = message;
      }
    }

    return currentNewestMessage;
  }

  /**
   * @returns oldest message in the array. `undefined` if given array is empty.
   */
  public static async getOldestMessage(messages: TimestampedMessage[]): Promise<TimestampedMessage | undefined> {
    let currentOldestMessage: TimestampedMessage | undefined = undefined;
    for (const message of messages) {
      if (currentOldestMessage === undefined || await Message.isOlder(message, currentOldestMessage)) {
        currentOldestMessage = message;
      }
    }

    return currentOldestMessage;
  }

  /**
   * Checks if first message is newer than second message.
   * @returns `true` if `a` is newer than `b`; `false` otherwise
   */
  public static async isNewer(a: TimestampedMessage, b: TimestampedMessage): Promise<boolean> {
    const aIsNewer = (await Message.compareMessageTimestamp(a, b) > 0);
    return aIsNewer;
  }

  /**
   * Checks if first message is older than second message.
   * @returns `true` if `a` is older than `b`; `false` otherwise
   */
  public static async isOlder(a: TimestampedMessage, b: TimestampedMessage): Promise<boolean> {
    const aIsOlder = (await Message.compareMessageTimestamp(a, b) < 0);
    return aIsOlder;
  }

  /**
   * Compares the `messageTimestamp` of the given messages with a fallback to message CID according to the spec.
   * @returns 1 if `a` is larger/newer than `b`; -1 if `a` is smaller/older than `b`; 0 otherwise (same age)
   */
  public static async compareMessageTimestamp(a: TimestampedMessage, b: TimestampedMessage): Promise<number> {
    if (a.descriptor.messageTimestamp > b.descriptor.messageTimestamp) {
      return 1;
    } else if (a.descriptor.messageTimestamp < b.descriptor.messageTimestamp) {
      return -1;
    }

    // else `messageTimestamp` is the same between a and b
    // compare the `dataCid` instead, the < and > operators compare strings in lexicographical order
    return Message.compareCid(a, b);
  }
}

function someString(): string { return 'foo'; }
async (): Promise<void> => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const parsed = await Message.parseSigned((await ProtocolsQuery.create({})).message);
  const s = someString();

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const created = await MessageBuilder.create(s, { } as ProtocolsQueryOptions);
};
