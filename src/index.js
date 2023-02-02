const AWS = require('aws-sdk');
const _values = require('lodash.values');
const _get = require('lodash.get');
const chalk = require('chalk');

const GLOBAL_TABLE_VERSION = '2019.11.21';
const MASTER_REGION = 'eu-west-2';

class CreateDynamoDBGlobalTables {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.options = options;
    this.hooks = {
      'after:deploy:deploy': this.createGlobalTables.bind(this),
    };
  }

  async createGlobalTables() {
    const enabled = _get(this.serverless, 'service.custom.dynamoDBGlobalTables.enabled', true);
    if (enabled === false) {
      this.log('Plugin disabled');
      return;
    }

    const provider = this.serverless.getProvider('aws');
    const region = provider.getRegion();
    const tableNames = this.getTableNames();

    if (region !== 'ca-central-1') {
      await Promise.all(tableNames.map(t => this.createGlobalTable(t, region)));
      await Promise.all(tableNames.map(t => this.addReplica(t, region)));
    }

    /**
     * We will need to somehow get the list of tableNames for the
     * version upgrades in a different manner as to how we get the tableNames above.
     * They won't be part of the stack for example, for ca-canada-1
     */
    const upgradeVersionTableNames = [
      'devmartn-user-tier-activities',
      'devmartn-user-tiers',
    ];

    await Promise.all(upgradeVersionTableNames.map(t => this.updateReplicaVersion(t)));
  }

  getTableNames() {
    const tableNames = _values(this.serverless.service.resources.Resources)
      .filter(r => r.Type === 'AWS::DynamoDB::Table')
      .map(r => r.Properties.TableName);

    return tableNames;
  }

  async createGlobalTable(tableName, region) {
    const tableHasBeenUpgraded = await this.tableIsAtVersionNumber(tableName);
    if (tableHasBeenUpgraded) {
      this.log(`Table ${tableName} has been upgraded to verision ${GLOBAL_TABLE_VERSION}`);

      return false;
    }
    const dynamo = new AWS.DynamoDB({ region });
    const params = {
      GlobalTableName: tableName,
      ReplicationGroup: [{ RegionName: region }]
    };

    try {
      await dynamo.createGlobalTable(params).promise();
      this.log(`Added Global Table ${tableName} with ${region} replica`);
    } catch (error) {
      if (error.code === 'GlobalTableAlreadyExistsException') {
        this.log(`Global table ${tableName} already exists`);
      } else {
        throw error;
      }
    }
  }

  async addReplica(tableName, region) {
    const tableHasBeenUpgraded = await this.tableIsAtVersionNumber(tableName);
    if (tableHasBeenUpgraded) {
      this.log(`Table ${tableName} has been upgraded to verision ${GLOBAL_TABLE_VERSION}`);

      return false;
    }
    this.log(`Adding replica for ${tableName} and ${region}`);
    const dynamo = new AWS.DynamoDB({ region });
    const params = {
      GlobalTableName: tableName,
      ReplicaUpdates: [
        { Create: { RegionName: region } }
      ]
    };

    try {
      await dynamo.updateGlobalTable(params).promise();
      this.log(`Added Replica ${tableName} to ${region}`);
    } catch (error) {
      if (error.code === 'ReplicaAlreadyExistsException') {
        this.log(`Replica ${tableName} already exists in ${region}`);
      } else {
        throw error;
      }
    }
  }

  async tableIsAtVersionNumber(tableName) {
    const dynamo = new AWS.DynamoDB({ region: MASTER_REGION });
    const params = {
      TableName: tableName,
    };

    let globalTableVersion = '';

    try {
      const tableDetails = await dynamo.describeTable(params).promise();
      globalTableVersion = tableDetails.Table.GlobalTableVersion;
    } catch (error) {
      this.log(error);
    }

    this.log(`Table ${tableName} is at version ${globalTableVersion}`);

    return globalTableVersion === GLOBAL_TABLE_VERSION;
  }

  async updateReplicaVersion(tableName) {

    /**
     * The regions can be defined in the yaml but we are just
     * using an array as an example here
     */

    const regions = ['ca-central-1', 'ap-southeast-2', 'us-east-1', 'us-east-2', MASTER_REGION]

    this.log(`Checking replica version for ${tableName}`);
    const canUpdate = await this.tableIsAtVersionNumber(tableName);
    if (!canUpdate) {
      this.log(`Replica cannot be updated for table ${tableName} and version ${GLOBAL_TABLE_VERSION}`);

      return false;
    }

    for (const region of regions) {
      if (region === MASTER_REGION) {
        this.log(`Skipping adding version ${GLOBAL_TABLE_VERSION} for the master region ${region}`);

        continue;
      }
      this.log(`Updating replica for ${tableName} and ${region} to version ${GLOBAL_TABLE_VERSION}`);

      const dynamo = new AWS.DynamoDB({region: MASTER_REGION});
      const params = {
        TableName: tableName,
        ReplicaUpdates: [
          {Create: {RegionName: region}}
        ]
      };

      try {
        await dynamo.updateTable(params).promise();
        this.log(`Added Replica ${tableName} to ${region}`);
      } catch (error) {
        const tableExistsMessage = 'one or more replicas already existed as tables.'
        if (error.code === 'ValidationException' && error.message.includes(tableExistsMessage)) {
          this.log(`Replica ${tableName} already exists in ${region}`);
        } else {
          throw error;
        }
      }
    }
  }

  log(message) {
    this.serverless.cli.consoleLog(`DynamoDB Global Tables: ${chalk.yellow(message)}`);
  }
}

module.exports = CreateDynamoDBGlobalTables;

