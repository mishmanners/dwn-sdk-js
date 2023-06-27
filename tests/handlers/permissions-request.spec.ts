
import { expect } from 'chai';
import sinon from 'sinon';

import { DataStoreLevel } from '../../src/store/data-store-level.js';
import { DidKeyResolver } from '../../src/did/did-key-resolver.js';
import { DidResolver } from '../../src/did/did-resolver.js';
import { Dwn } from '../../src/dwn.js';
import { EventLogLevel } from '../../src/event-log/event-log-level.js';
import { Message } from '../../src/core/message.js';
import { MessageStoreLevel } from '../../src/store/message-store-level.js';
import { PermissionsRequest } from '../../src/interfaces/permissions-request.js';
import { PermissionsRequestHandler } from '../../src/handlers/permissions-request.js';
import { TestDataGenerator } from '../utils/test-data-generator.js';
import { DwnInterfaceName, DwnMethodName } from '../../src/index.js';

describe('PermissionsRequestHandler.handle()', () => {
  let didResolver: DidResolver;
  let messageStore: MessageStoreLevel;
  let dataStore: DataStoreLevel;
  let eventLog: EventLogLevel;
  let dwn: Dwn;

  describe('functional tests', () => {
    before(async () => {
      didResolver = new DidResolver([new DidKeyResolver()]);

      // important to follow this pattern to initialize and clean the message and data store in tests
      // so that different suites can reuse the same block store and index location for testing
      messageStore = new MessageStoreLevel({
        blockstoreLocation : 'TEST-MESSAGESTORE',
        indexLocation      : 'TEST-INDEX'
      });

      dataStore = new DataStoreLevel({
        blockstoreLocation: 'TEST-DATASTORE'
      });

      eventLog = new EventLogLevel({
        location: 'TEST-EVENTLOG'
      });

      dwn = await Dwn.create({ didResolver, messageStore, dataStore, eventLog });
    });

    beforeEach(async () => {
      sinon.restore(); // wipe all previous stubs/spies/mocks/fakes

      // clean up before each test rather than after so that a test does not depend on other tests to do the clean up
      await messageStore.clear();
      await dataStore.clear();
      await eventLog.clear();
    });

    after(async () => {
      await dwn.close();
    });

    it('should accept a PermissionsRequest with conditions omitted', async () => {
      // scenario: Bob sends a PermissionsRequest to Alice's DWN
      const alice = await DidKeyResolver.generate();
      const bob = await DidKeyResolver.generate();

      const { message } = await TestDataGenerator.generatePermissionsRequest({
        author      : bob,
        description : 'Please allow me to RecordsWrite',
        grantedBy   : alice.did,
        grantedTo   : bob.did,
        grantedFor  : alice.did,
        scope       : {
          interface : DwnInterfaceName.Records,
          method    : DwnMethodName.Write,
        },
      });

      const reply = await dwn.processMessage(alice.did, message);
      expect(reply.status.code).to.equal(202);
    });

    it('should return 401 if auth fails', async () => {
      const alice = await DidKeyResolver.generate();
      alice.keyId = 'wrongValue'; // to fail authentication
      const { message } = await TestDataGenerator.generatePermissionsRequest({
        author : alice,
        scope  : {
          interface : DwnInterfaceName.Records,
          method    : DwnMethodName.Write,
        }
      });

      const reply = await dwn.processMessage(alice.did, message);
      expect(reply.status.code).to.equal(401);
      expect(reply.status.detail).to.contain('not a valid DID');
    });

    it('should return 400 if failure parsing the message', async () => {
      const alice = await DidKeyResolver.generate();
      const { message } = await TestDataGenerator.generatePermissionsRequest();

      const permissionsRequestHandler = new PermissionsRequestHandler(didResolver, messageStore, eventLog);

      // stub the `parse()` function to throw an error
      sinon.stub(PermissionsRequest, 'parse').throws('anyError');
      const reply = await permissionsRequestHandler.handle({ tenant: alice.did, message });

      expect(reply.status.code).to.equal(400);
    });

    describe('event log', () => {
      it('should add event for PermissionsRequest', async () => {
        const alice = await DidKeyResolver.generate();
        const { message } = await TestDataGenerator.generatePermissionsRequest({
          author : alice,
          scope  : {
            interface : DwnInterfaceName.Records,
            method    : DwnMethodName.Write,
          },
        });

        const reply = await dwn.processMessage(alice.did, message);
        expect(reply.status.code).to.equal(202);

        const events = await eventLog.getEvents(alice.did);
        expect(events.length).to.equal(1);

        const messageCid = await Message.getCid(message);
        expect(events[0].messageCid).to.equal(messageCid);
      });

      it('should not add a new event if we have already stored this PermissionsRequest', async () => {
        const alice = await DidKeyResolver.generate();
        const { message } = await TestDataGenerator.generatePermissionsRequest({
          author : alice,
          scope  : {
            interface : DwnInterfaceName.Records,
            method    : DwnMethodName.Write,
          },
        });

        let reply = await dwn.processMessage(alice.did, message);
        expect(reply.status.code).to.equal(202);

        reply = await dwn.processMessage(alice.did, message);
        expect(reply.status.code).to.equal(202);

        const events = await eventLog.getEvents(alice.did);
        expect(events.length).to.equal(1);

        const messageCid = await Message.getCid(message);
        expect(events[0].messageCid).to.equal(messageCid);
      });
    });
  });
});