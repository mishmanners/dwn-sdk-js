import type { Filter, MessageSort, Pagination, RangeFilter } from '../types/message-types.js';
import type { LevelWrapperBatchOperation, LevelWrapperIteratorOptions } from './level-wrapper.js';

import { flatten } from '../utils/object.js';
import { SortOrder } from '../types/message-types.js';
import { createLevelDatabase, LevelWrapper } from './level-wrapper.js';

export interface IndexLevelOptions {
  signal?: AbortSignal;
}

interface SortIndexes {
  messageTimestamp?: string
  dateCreated?: string
  datePublished?: string
}

/**
 * A LevelDB implementation for indexing the messages stored in the DWN.
 */
export class IndexLevel {
  config: IndexLevelConfig;

  db: LevelWrapper<string>;

  constructor(config: IndexLevelConfig) {
    this.config = {
      createLevelDatabase,
      ...config
    };

    this.db = new LevelWrapper<string>({ ...this.config, valueEncoding: 'utf8' });
  }

  async open(): Promise<void> {
    return this.db.open();
  }

  async close(): Promise<void> {
    return this.db.close();
  }

  private matchPrefix(propertyName: string, sort: MessageSort = {}, propertyValue?: unknown): string {
    const parts = [];
    const { messageTimestamp, dateCreated, datePublished } = sort;

    if (dateCreated !== undefined) {
      parts.push( dateCreated === SortOrder.Ascending ? '__dateCreated' : '__dateCreated_descending', propertyName);
    } else if (datePublished !== undefined) {
      parts.push( datePublished === SortOrder.Ascending ? '__datePublished' : '__datePublished_descending', propertyName);
    } else if (messageTimestamp !== undefined && messageTimestamp === SortOrder.Descending) {
      parts.push('__descending', propertyName);
    } else {
      parts.push(propertyName);
    }

    if (propertyValue !== undefined) {
      parts.push(this.encodeValue(propertyValue));
    }

    return this.join(...parts);
  }

  /**
   * Adds indexes for a specific data/object/content.
   * @param dataId ID of the data/object/content being indexed.
   */
  async put(
    dataId: string,
    indexes: { [property: string]: unknown },
    options?: IndexLevelOptions
  ): Promise<void> {

    indexes = flatten(indexes);

    const operations: LevelWrapperBatchOperation<string>[] = [ ];

    // create an index entry for each property in the `indexes`
    for (const propertyName in indexes) {
      const propertyValue = indexes[propertyName];
      operations.push(...this.indexOperations(propertyName, propertyValue, dataId, this.sortIndexes(indexes)));
    }

    // create a reverse lookup entry for data ID -> its indexes
    // this is for indexes deletion (`delete()`): so that given the data ID, we are able to delete all its indexes
    // we can consider putting this info in a different data partition if this ever becomes more complex/confusing
    operations.push({ type: 'put', key: `__${dataId}__indexes`, value: JSON.stringify(indexes) });

    await this.db.batch(operations, options);
  }

  private indexOperations(propertyName: string, propertyValue: unknown, dataId: string, sort: SortIndexes): LevelWrapperBatchOperation<string>[] {
    // should probably fail if there is no messageTimestamp?
    const { messageTimestamp, dateCreated, datePublished } = sort;

    const operations: LevelWrapperBatchOperation<string>[] = [ ];

    const key = this.join(propertyName, this.encodeValue(propertyValue), messageTimestamp || '', dataId); // default
    operations.push({ type: 'put', key, value: dataId });
    const descendingKey = this.join('__descending', propertyName, this.encodeValue(propertyValue), this.descendingTimestampKey(messageTimestamp), dataId);
    operations.push({ type: 'put', key: descendingKey, value: dataId });

    // no need to double-index the dates the dates themselves
    if (dateCreated !== undefined) {
      const key = this.join('__dateCreated', propertyName, this.encodeValue(propertyValue), dateCreated, dataId);
      operations.push({ type: 'put', key, value: dataId });
      const descendingKey = this.join('__dateCreated_descending', propertyName, this.encodeValue(propertyValue), this.descendingTimestampKey(dateCreated), dataId);
      operations.push({ type: 'put', key: descendingKey, value: dataId });
    }

    // no need to double-index the dates the dates themselves
    if (datePublished !== undefined) {
      const key = this.join('__datePublished', propertyName, this.encodeValue(propertyValue), datePublished, dataId);
      operations.push({ type: 'put', key, value: dataId });

      const descendingKey = this.join('__datePublished_descending', propertyName, this.encodeValue(propertyValue), this.descendingTimestampKey(datePublished), dataId);
      operations.push({ type: 'put', key: descendingKey, value: dataId });
    }
    return operations;
  }

  private descendingTimestampKey(timeStamp?: string): string {
    if (timeStamp === undefined) {
      return '';
    }
    const unixTime = Date.parse(timeStamp);
    if (isNaN(unixTime)) {
      return '';
    }

    return (Number.MAX_SAFE_INTEGER - unixTime).toString().padStart(16, '0');
  }

  private sortIndexes(indexes: { [property: string]: unknown}): SortIndexes {
    const { messageTimestamp, dateCreated, datePublished } = indexes;

    return {
      messageTimestamp : messageTimestamp ? messageTimestamp as string : undefined,
      dateCreated      : dateCreated ? dateCreated as string : undefined,
      datePublished    : datePublished ? datePublished as string : undefined,
    };
  }

  async query(
    filter: Filter,
    dateSort: MessageSort,
    pagination?: Pagination,
    options?: IndexLevelOptions,
  ): Promise<Array<string>> {
    // Note: We have an array of Promises in order to support OR (anyOf) matches when given a list of accepted values for a property
    const propertyNameToPromises: { [key: string]: Promise<string[]>[] } = {};

    // Do a separate DB query for each property in `filter`
    // We will find the union of these many individual queries later.
    for (const propertyName in filter) {
      const propertyFilter = filter[propertyName];

      if (typeof propertyFilter === 'object') {
        if (Array.isArray(propertyFilter)) {
          // `propertyFilter` is a AnyOfFilter

          // Support OR matches by querying for each values separately,
          // then adding them to the promises associated with `propertyName`
          propertyNameToPromises[propertyName] = [];
          for (const propertyValue of new Set(propertyFilter)) {
            const exactMatchesPromise = this.findExactMatches(propertyName, propertyValue, dateSort, options);
            propertyNameToPromises[propertyName].push(exactMatchesPromise);
          }
        } else {
          // `propertyFilter` is a `RangeFilter`
          const rangeMatchesPromise = this.findRangeMatches(propertyName, propertyFilter, dateSort, options);
          propertyNameToPromises[propertyName] = [rangeMatchesPromise];
        }
      } else {
        // propertyFilter is an EqualFilter, meaning it is a non-object primitive type
        const exactMatchesPromise = this.findExactMatches(propertyName, propertyFilter, dateSort, options);
        propertyNameToPromises[propertyName] = [exactMatchesPromise];
      }
    }

    // map of ID of all data/object -> list of missing property matches
    // if count of missing property matches is 0, it means the data/object fully matches the filter
    const missingPropertyMatchesForId: { [dataId: string]: Set<string> } = { };

    // Resolve promises and find the union of results for each individual propertyName DB query
    const matchedIDs: string[] = [ ];
    for (const [propertyName, promises] of Object.entries(propertyNameToPromises)) {
      // acting as an OR match for the property, any of the promises returning a match will be treated as a property match
      for (const promise of promises) {
        for (const dataId of await promise) {
          // if first time seeing a property matching for the data/object, record all properties needing a match to track progress
          missingPropertyMatchesForId[dataId] ??= new Set<string>([ ...Object.keys(filter) ]);

          missingPropertyMatchesForId[dataId].delete(propertyName);
          if (missingPropertyMatchesForId[dataId].size === 0) {
            // full filter match, add it to return list
            matchedIDs.push(dataId);
          }
        }
      }
    }

    return matchedIDs;
  }

  async delete(dataId: string, options?: IndexLevelOptions): Promise<void> {
    const serializedIndexes = await this.db.get(`__${dataId}__indexes`, options);
    if (!serializedIndexes) {
      return;
    }

    const indexes = JSON.parse(serializedIndexes);
    const { messageTimestamp, dateCreated, datePublished } = this.sortIndexes(indexes);
    // delete all indexes associated with the data of the given ID
    const ops: LevelWrapperBatchOperation<string>[] = [ ];
    for (const propertyName in indexes) {
      const propertyValue = indexes[propertyName];
      const key = this.join(propertyName, this.encodeValue(propertyValue), messageTimestamp || '', dataId);
      ops.push({ type: 'del', key });
      const descendingKey = this.join('__descending', propertyName, this.encodeValue(propertyValue), this.descendingTimestampKey(messageTimestamp), dataId);
      ops.push({ type: 'del', key: descendingKey });
      if (dateCreated !== undefined) {
        const key = this.join('__dateCreated', propertyName, this.encodeValue(propertyValue), dateCreated, dataId);
        ops.push({ type: 'del', key });
        const descendingKey = this.join('__dateCreated_descending', propertyName, this.encodeValue(propertyValue), this.descendingTimestampKey(dateCreated), dataId);
        ops.push({ type: 'del', key: descendingKey });
      }
      if (datePublished !== undefined) {
        const key = this.join('__datePublished', propertyName, this.encodeValue(propertyValue), datePublished, dataId);
        ops.push({ type: 'del', key });
        const descendingKey = this.join('__datePublished_descending', propertyName, this.encodeValue(propertyValue), this.descendingTimestampKey(datePublished), dataId);
        ops.push({ type: 'del', key: descendingKey });
      }
    }

    ops.push({ type: 'del', key: `__${dataId}__indexes` });

    await this.db.batch(ops, options);
  }

  async clear(): Promise<void> {
    return this.db.clear();
  }

  /**
   * @returns IDs of data that matches the exact property and value.
   */
  private async findExactMatches(propertyName: string, propertyValue: unknown, sort: MessageSort, options?: IndexLevelOptions): Promise<string[]> {
    const propertyValuePrefix = this.matchPrefix(propertyName, sort, propertyValue);

    const iteratorOptions: LevelWrapperIteratorOptions<string> = {
      gte: propertyValuePrefix
    };

    const matches: string[] = [];
    for await (const [ key, dataId ] of this.db.iterator(iteratorOptions, options)) {
      if (!key.startsWith(propertyValuePrefix)) {
        break;
      }

      matches.push(dataId);
    }
    return matches;
  }

  /**
   * @returns IDs of data that matches the range filter.
   */
  private async findRangeMatches(
    propertyName: string,
    rangeFilter: RangeFilter,
    sort: MessageSort,
    options?: IndexLevelOptions
  ): Promise<string[]> {
    const iteratorOptions: LevelWrapperIteratorOptions<string> = {};

    const { order, field } = IndexLevel.extractSortInfo(sort);
    //always sort in ascending order for ranges and use the reverse iterator.
    const prefixOrder = order === SortOrder.Descending ? { [field]: SortOrder.Ascending } : sort;
    const propertyPrefix = this.matchPrefix(propertyName, prefixOrder);

    for (const comparator in rangeFilter) {
      const comparatorName = comparator as keyof RangeFilter;
      const comparatorValue = rangeFilter[comparatorName];
      if (!comparatorValue) {
        continue;
      }
      iteratorOptions[comparatorName] = this.matchPrefix(propertyName, prefixOrder, comparatorValue);
    }

    // if there is no lower bound specified (`gt` or `gte`), we need to iterate from the upper bound,
    // so that we will iterate over all the matches before hitting mismatches.
    if (iteratorOptions.gt === undefined && iteratorOptions.gte === undefined && order !== SortOrder.Descending) {
      iteratorOptions.reverse = true;
    } else if (order === SortOrder.Descending) {
      iteratorOptions.reverse = true;
    }

    let lteValue: string | undefined;
    if ('lte' in rangeFilter) {
      for (const dataId of await this.findExactMatches(propertyName, rangeFilter.lte, sort, options)) {
        lteValue = dataId;
      }
    }

    const matches: string[] = [];
    if (lteValue && order === SortOrder.Descending) {
      matches.push(lteValue);
    }

    for await (const [ key, dataId ] of this.db.iterator(iteratorOptions, options)) {
      // if "greater-than" is specified, skip all keys that contains the exact value given in the "greater-than" condition
      if ('gt' in rangeFilter && IndexLevel.extractValueFromKey(key) === this.encodeValue(rangeFilter.gt)) {
        continue;
      }

      // immediately stop if we arrive at an index entry for a different property
      if (!key.startsWith(propertyPrefix)) {
        break;
      }

      matches.push(dataId);
    }

    if (lteValue && order === SortOrder.Ascending) {
      matches.push(lteValue);
    }

    return matches;
  }

  private encodeValue(value: unknown): string {
    switch (typeof value) {
    case 'string':
      // We can't just `JSON.stringify` as that'll affect the sort order of strings.
      // For example, `'\x00'` becomes `'\\u0000'`.
      return `"${value}"`;
    case 'number':
      return IndexLevel.encodeNumberValue(value);
    default:
      return String(value);
    }
  }

  private static extractSortInfo(sort: MessageSort): { order: SortOrder, field: string} {
    const key = Object.keys(sort).at(0);
    if (key === undefined) {
      return { order: SortOrder.Ascending, field: 'messageTimestamp' };
    }
    return { order: sort[key], field: key };
  }

  /**
   *  Encodes a numerical value as a string for lexicographical comparison.
   *  If the number is positive it simply pads it with leading zeros.
   *  ex.: input:  1024 => "0000000000001024"
   *       input: -1024 => "!9007199254739967"
   *
   * @param value the number to encode.
   * @returns a string representation of the number.
   */
  static encodeNumberValue(value: number): string {
    const NEGATIVE_OFFSET = Number.MAX_SAFE_INTEGER;
    const NEGATIVE_PREFIX = '!'; // this will be sorted below positive numbers lexicographically
    const PADDING_LENGTH = String(Number.MAX_SAFE_INTEGER).length;

    const prefix: string = value < 0 ? NEGATIVE_PREFIX : '';
    const offset: number = value < 0 ? NEGATIVE_OFFSET : 0;
    return prefix + String(value + offset).padStart(PADDING_LENGTH, '0');
  }

  /**
   * Extracts the value encoded within the indexed key when a record is inserted.
   *
   * ex. key: 'dateCreated\u0000"2023-05-25T18:23:29.425008Z"\u0000bafyreigs3em7lrclhntzhgvkrf75j2muk6e7ypq3lrw3ffgcpyazyw6pry'
   *     extracted value: "2023-05-25T18:23:29.425008Z"
   *
   * @param key an IndexLevel db key.
   * @returns the extracted encodedValue from the key.
   */
  static extractValueFromKey(key: string): string {
    const [, value] = key.split(this.delimiter);
    return value;
  }

  /**
   * Joins the given values using the `\x00` (\u0000) character.
   */
  private static delimiter = `\x00`;
  private join(...values: unknown[]): string {
    return values.join(IndexLevel.delimiter);
  }

  async dump(): Promise<void> {
    console.group('db');
    await this.db['dump']?.();
    console.groupEnd();
  }
}

type IndexLevelConfig = {
  location: string,
  createLevelDatabase?: typeof createLevelDatabase,
};