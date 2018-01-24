/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

const util = require('util');
const path = require('path');
const assert = require('assert');
const spawn = require('child_process').spawn;
const Client = require('dse-driver').Client;
const dseGraph = require('../');

function noop () {}

const helper = {
  getDseVersion: function() {
    let version = process.env['TEST_DSE_VERSION'];
    if (!version) {
      version = '5.0.3';
    }
    return version;
  },
  /**
   * Determines if the current DSE instance version is greater than or equals to the version provided
   * @param {String} version The version in string format, dot separated.
   * @returns {Boolean}
   */
  isDseGreaterThan: function (version) {
    return helper.versionCompare(helper.getDseVersion(), version);
  },
  /**
   * Determines if the current DSE instance version is greater than or equals to the version provided
   * @param {String} instanceVersionStr The version of the current instance.
   * @param {String} version The version in string format, dot separated.
   * @returns {Boolean}
   */
  versionCompare: function (instanceVersionStr, version) {
    let expected = [1, 0]; //greater than or equals to
    if (version.indexOf('<=') === 0) {
      version = version.substr(2);
      expected = [-1, 0]; //less than or equals to
    }
    else if (version.indexOf('<') === 0) {
      version = version.substr(1);
      expected = [-1]; //less than
    }
    const instanceVersion = instanceVersionStr.split('.').map(function (x) { return parseInt(x, 10);});
    const compareVersion = version.split('.').map(function (x) { return parseInt(x, 10) || 0;});
    for (let i = 0; i < compareVersion.length; i++) {
      const compare = compareVersion[i] || 0;
      if (instanceVersion[i] > compare) {
        //is greater
        return expected.indexOf(1) >= 0;
      }
      else if (instanceVersion[i] < compare) {
        //is smaller
        return expected.indexOf(-1) >= 0;
      }
    }
    //are equal
    return expected.indexOf(0) >= 0;
  },
  queries: {
    basic: 'SELECT key FROM system.local',
    graph: {
      modernSchema:
        'schema.config().option("graph.schema_mode").set("production")\n' +
        'schema.config().option("graph.allow_scan").set("true")\n' +
        'schema.propertyKey("name").Text().ifNotExists().create();\n' +
        'schema.propertyKey("age").Int().ifNotExists().create();\n' +
        'schema.propertyKey("lang").Text().ifNotExists().create();\n' +
        'schema.propertyKey("weight").Float().ifNotExists().create();\n' +
        'schema.vertexLabel("person").properties("name", "age").ifNotExists().create();\n' +
        'schema.vertexLabel("software").properties("name", "lang").ifNotExists().create();\n' +
        'schema.edgeLabel("created").properties("weight").connection("person", "software").ifNotExists().create();\n' +
        'schema.edgeLabel("knows").properties("weight").connection("person", "person").ifNotExists().create();',
      modernGraph:
        'Vertex marko = graph.addVertex(label, "person", "name", "marko", "age", 29);\n' +
        'Vertex vadas = graph.addVertex(label, "person", "name", "vadas", "age", 27);\n' +
        'Vertex lop = graph.addVertex(label, "software", "name", "lop", "lang", "java");\n' +
        'Vertex josh = graph.addVertex(label, "person", "name", "josh", "age", 32);\n' +
        'Vertex ripple = graph.addVertex(label, "software", "name", "ripple", "lang", "java");\n' +
        'Vertex peter = graph.addVertex(label, "person", "name", "peter", "age", 35);\n' +
        'marko.addEdge("knows", vadas, "weight", 0.5f);\n' +
        'marko.addEdge("knows", josh, "weight", 1.0f);\n' +
        'marko.addEdge("created", lop, "weight", 0.4f);\n' +
        'josh.addEdge("created", ripple, "weight", 1.0f);\n' +
        'josh.addEdge("created", lop, "weight", 0.4f);\n' +
        'peter.addEdge("created", lop, "weight", 0.2f);',
      metaPropsSchema:
        'schema.propertyKey("sub_prop").Text().create()\n' +
        'schema.propertyKey("sub_prop2").Text().create()\n' +
        'schema.propertyKey("meta_prop").Text().properties("sub_prop", "sub_prop2").create()\n' +
        'schema.vertexLabel("meta_v").properties("meta_prop").create()\n',
      multiCardinalitySchema:
        'schema.propertyKey("multi_prop").Text().multiple().create()\n' +
        'schema.vertexLabel("multi_v").properties("multi_prop").create()',
      getAddressBookSchema: function (dseVersion) {
        return (
          'schema.config().option("graph.schema_mode").set("production")\n' +
          'schema.config().option("graph.allow_scan").set("true")\n' +
          'schema.propertyKey("full_name").Text().create()\n' +
          'schema.propertyKey("coordinates").' + pointType(dseVersion) + '.create()\n' +
          'schema.propertyKey("city").Text().create()\n' +
          'schema.propertyKey("state").Text().create()\n' +
          'schema.propertyKey("description").Text().create()\n' +
          'schema.propertyKey("alias").Text().create()\n' +
          'schema.vertexLabel("user")' +
          '.properties("full_name", "coordinates", "city", "state", "description").create()\n' +
          'schema.vertexLabel("user").index("search").search().by("full_name").asString().by("coordinates")' +
          '.by("description").asText().by("alias").asString().add()\n'
        );
      },
      addressBookGraph: [
        'g.addV("user").property("full_name", "Paul Thomas Joe").property("city", "Rochester")' +
          '.property("state", "MN").property("coordinates", Geo.point(-92.46295, 44.0234))' +
          '.property("description", "Lives by the hospital").property("alias", "mario")',
        'g.addV("user").property("full_name", "George Bill Steve").property("city", "Minneapolis")' +
          '.property("state", "MN").property("coordinates", Geo.point(-93.266667, 44.9778))' +
          '.property("description", "A cold dude").property("alias", "wario")',
        'g.addV("user").property("full_name", "James Paul Joe").property("city", "Chicago")' +
          '.property("state", "IL").property("coordinates", Geo.point(-87.684722, 41.836944))' +
          '.property("description", "Likes to hang out").property("alias", "bowser")',
        'g.addV("user").property("full_name", "Jill Alice").property("city", "Atlanta")' +
          '.property("state", "GA").property("coordinates", Geo.point(-84.39, 33.755))' +
          '.property("description", "Enjoys a very nice cold coca cola").property("alias", "peach")'
      ]
    }
  },
  ipPrefix: '127.0.0.',
  assertInstanceOf: function (instance, constructor) {
    assert.notEqual(instance, null, 'Expected instance, obtained ' + instance);
    assert.ok(instance instanceof constructor, 'Expected instance of ' + constructor.name + ', actual constructor: ' + instance.constructor.name);
  },
  assertBufferString: function (instance, textValue) {
    this.assertInstanceOf(instance, Buffer);
    assert.strictEqual(instance.toString(), textValue);
  },
  /**
   * @param {ResultSet} result
   */
  keyedById: function (result) {
    const map = {};
    const columnKeys = result.columns.map(function (c) { return c.name;});
    if (columnKeys.indexOf('id') < 0 || columnKeys.indexOf('value') < 0) {
      throw new Error('ResultSet must contain the columns id and value');
    }
    result.rows.forEach(function (row) {
      map[row['id']] = row['value'];
    });
    return map;
  },
  noop: function () {},
  throwOp: function (err) {
    if (err) {
      throw err;
    }
  },
  /**
   * Version dependent it() method for mocha test case
   * @param {String} testVersion Minimum version of Cassandra needed for this test
   * @param {String} testCase Test case name
   * @param {Function} func
   */
  vit: function (testVersion, testCase, func) {
    executeIfVersion(testVersion, it, [testCase, func]);
  },

  /**
   * Version dependent describe() method for mocha test case
   * @param {String} testVersion Minimum version of Cassandra needed for this test
   * @param {String} title Title of the describe section.
   * @param {Function} func
   */
  vdescribe: function (testVersion, title, func) {
    executeIfVersion(testVersion, describe, [title, func]);
  },
  baseOptions: {
    contactPoints: ['127.0.0.1']
  },
  /**
   * @returns {Function} A function with a single callback param, applying the fn with parameters
   */
  toTask: function (fn, context) {
    const params = Array.prototype.slice.call(arguments, 2);
    return (function (next) {
      params.push(next);
      fn.apply(context, params);
    });
  },
  /**
   * Determines if test tracing is enabled
   */
  isTracing: function () {
    return (process.env['TEST_TRACE'] === 'on');
  },
  trace: function (format) {
    if (!helper.isTracing()) {
      return;
    }
    // eslint-disable-next-line
    console.log('\t...' + util.format.apply(null, arguments));
  },
  wait: function (ms, callback) {
    if (!ms) {
      ms = 0;
    }
    return (function (err) {
      if (err) {
        return callback(err);
      }
      setTimeout(callback, ms);
    });
  },
  extend: function (target) {
    const sources = Array.prototype.slice.call(arguments, 1);
    sources.forEach(function (source) {
      for (const prop in source) {
        if (source.hasOwnProperty(prop)) {
          target[prop] = source[prop];
        }
      }
    });
    return target;
  },
  getOptions: function (options) {
    return helper.extend({}, helper.baseOptions, options);
  },
  /**
   * Connects to the cluster, makes a few queries and shutsdown the client
   * @param {Client} client
   * @param {Function} callback
   */
  connectAndQuery: function (client, callback) {
    const self = this;
    series([
      client.connect.bind(client),
      function doSomeQueries(next) {
        timesSeries(10, function (n, timesNext) {
          client.execute(self.queries.basic, timesNext);
        }, next);
      },
      client.shutdown.bind(client)
    ], callback);
  },
  /**
   * Identifies the host that is the spark master (the one that is listening on port 7077)
   * and returns it.
   * @param {Client} client instance that contains host metadata.
   * @param {Function} callback invoked with the host that is the spark master or error.
   */
  findSparkMaster: function (client, callback) {
    client.execute('call DseClientTool.getAnalyticsGraphServer();', function(err, result) {
      if(err) {
        callback(err);
      }
      const row = result.first();
      const host = row.result.ip;
      callback(null, host);
    });
  },
  requireOptional: function (moduleName) {
    try {
      // eslint-disable-next-line global-require
      return require(moduleName);
    }
    catch (err) {
      if (err.code === 'MODULE_NOT_FOUND') {
        return null;
      }
      throw err;
    }
  },
  conditionalDescribe: function (condition, text) {
    if (condition) {
      return describe;
    }
    return (function xdescribeWithText(name, fn) {
      return xdescribe(util.format('%s [%s]', name, text), fn);
    });
  },
  createModernGraph: function (name) {
    return (function (callback) {
      const client = new Client(helper.getOptions());
      series([
        client.connect.bind(client),
        function testCqlQuery(next) {
          client.execute(helper.queries.basic, next);
        },
        function createGraph(next) {
          client.executeGraph('system.graph(name).ifNotExists().create()', {name: name}, {graphName: null}, next);
        },
        function _createSchema(next) {
          client.executeGraph(helper.queries.graph.modernSchema, null, {graphName: name}, next);
        },
        function _loadData(next) {
          client.executeGraph(helper.queries.graph.modernGraph, null, {graphName: name}, next);
        },
        client.shutdown.bind(client)
      ], callback);
    });
  },
  wrapTraversal: function (handler, options) {
    return helper.wrapClient(function (client, next) {
      const g = dseGraph.traversalSource(client);
      handler(g, next);
    }, options);
  },
  wrapClient: function (handler, options) {
    return (function wrappedTestCase(done) {
      const opts = helper.getOptions(helper.extend(options || {}, {
        graphOptions : { name: 'name1' },
        profiles: [
          dseGraph.createExecutionProfile('traversal', {})
        ]
      }));
      const client = new Client(opts);
      helper.series([
        client.connect.bind(client),
        function testItem(next) {
          handler(client, next);
        }
      ], function seriesFinished(err) {
        // Shutdown regardless of the result
        client.shutdown(function shutdownCallback() {
          done(err);
        });
      });
    });
  },
  ccm: {},
  ads: {}
};

function pointType(dseVersion) {
  return dseVersion.indexOf('5.0.') === 0 ? 'Point()' : 'Point().withGeoBounds()';
}

/**
 * Removes previous and creates a new cluster (create, populate and start)
 * @param {Number|String} nodeLength number of nodes in the cluster. If multiple dcs, use the notation x:y:z:...
 * @param {{[vnodes]: Boolean, [yaml]: Array.<String>, [jvmArgs]: Array.<String>, [ssl]: Boolean,
 *  [dseYaml]: Array.<String>, [workloads]: Array.<String>}|null} options
 * @param {Function} callback
 */
helper.ccm.startAll = function (nodeLength, options, callback) {
  const self = this;
  options = options || {};
  const version = helper.getDseVersion();
  helper.trace('Starting test DSE cluster v%s with %s node(s)', version, nodeLength);
  series([
    function (next) {
      //it wont hurt to remove
      self.exec(['remove'], function () {
        //ignore error
        next();
      });
    },
    function (next) {
      let create = ['create', 'test', '--dse', '-v', version];
      if (process.env['TEST_DSE_DIR']) {
        create = ['create', 'test', '--install-dir=' + process.env['TEST_DSE_DIR']];
        helper.trace('With', create[2]);
      }
      if (options.ssl) {
        create.push('--ssl', self.getPath('ssl'));
      }
      self.exec(create, helper.wait(options.sleep, next));
    },
    function (next) {
      const populate = ['populate', '-n', nodeLength.toString()];
      if (options.vnodes) {
        populate.push('--vnodes');
      }
      self.exec(populate, helper.wait(options.sleep, next));
    },
    function (next) {
      if (!options.yaml || !options.yaml.length) {
        return next();
      }
      helper.trace('With cassandra yaml options', options.yaml);
      self.exec(['updateconf'].concat(options.yaml), next);
    },
    function (next) {
      if (!options.dseYaml || !options.dseYaml.length) {
        return next();
      }
      helper.trace('With dse yaml options', options.dseYaml);
      self.exec(['updatedseconf'].concat(options.dseYaml), next);
    },
    function (next) {
      if (!options.workloads || !options.workloads.length) {
        return next();
      }
      helper.trace('With workloads', options.workloads);
      timesSeries(nodeLength, function(n, timesNext) {
        self.exec(['node'+ (n+1), 'setworkload', options.workloads.join(',')], timesNext);
      }, function(err) {
        next(err);
      });
    },
    function (next) {
      const start = ['start', '--wait-for-binary-proto'];
      if (util.isArray(options.jvmArgs)) {
        options.jvmArgs.forEach(function (arg) {
          start.push('--jvm_arg', arg);
        }, this);
        helper.trace('With jvm args', options.jvmArgs);
      }
      self.exec(start, helper.wait(options.sleep, next));
    },
    self.waitForUp.bind(self)
  ], function (err) {
    callback(err);
  });
};

/**
 * @param {Number|String} nodeLength number of nodes in the cluster. If multiple dcs, use the notation x:y:z:...
 * @param {{[vnodes]: Boolean, [yaml]: Array.<String>, [jvmArgs]: Array.<String>, [ssl]: Boolean,
 *  [dseYaml]: Array.<String>, [workloads]: Array.<String>}|null} options
 */
helper.ccm.startAllTask = function (nodeLength, options) {
  return (function startAllTaskFunc(callback) {
    helper.ccm.startAll(nodeLength, options, callback);
  });
};

/**
 * Adds a new node to the cluster
 * @param {Number} nodeIndex 1 based index of the node
 * @param {Function} callback
 */
helper.ccm.bootstrapNode = function (nodeIndex, callback) {
  const ipPrefix = helper.ipPrefix;
  helper.trace('bootstrapping node', nodeIndex);
  helper.ccm.exec([
    'add',
    'node' + nodeIndex,
    '-i',
    ipPrefix + nodeIndex,
    '-j',
    (7000 + 100 * nodeIndex).toString(),
    '-b',
    '--dse'
  ], callback);
};

/**
 * Sets the workload(s) for a given node.
 * @param {Number} nodeIndex 1 based index of the node
 * @param {Array<String>} workloads workloads to set.
 * @param {Function} callback
 */
helper.ccm.setWorkload = function (nodeIndex, workloads, callback) {
  helper.trace('node', nodeIndex, 'with workloads', workloads);
  helper.ccm.exec([
    'node' + nodeIndex,
    'setworkload',
    workloads.join(',')
  ], callback);
};

/**
 * @param {Number} nodeIndex 1 based index of the node
 * @param {Function} callback
 */
helper.ccm.startNode = function (nodeIndex, callback) {
  helper.ccm.exec(['node' + nodeIndex, 'start', '--wait-other-notice', '--wait-for-binary-proto'], callback);
};

/**
 * @param {Number} nodeIndex 1 based index of the node
 * @param {Function} callback
 */
helper.ccm.stopNode = function (nodeIndex, callback) {
  helper.ccm.exec(['node' + nodeIndex, 'stop'], callback);
};

helper.ccm.exec = function (params, callback) {
  this.spawn('ccm', params, callback);
};

helper.ccm.spawn = function (processName, params, callback) {
  if (!callback) {
    callback = function () {};
  }
  params = params || [];
  const originalProcessName = processName;
  if (process.platform.indexOf('win') === 0) {
    params = ['/c', processName].concat(params);
    processName = 'cmd.exe';
  }
  const p = spawn(processName, params);
  const stdoutArray= [];
  const stderrArray= [];
  let closing = 0;
  p.stdout.setEncoding('utf8');
  p.stderr.setEncoding('utf8');
  p.stdout.on('data', function (data) {
    stdoutArray.push(data);
  });

  p.stderr.on('data', function (data) {
    stderrArray.push(data);
  });

  p.on('close', function (code) {
    if (closing++ > 0) {
      //avoid calling multiple times
      return;
    }
    const info = {code: code, stdout: stdoutArray, stderr: stderrArray};
    let err = null;
    if (code !== 0) {
      err = new Error(
        'Error executing ' + originalProcessName + ':\n' +
        info.stderr.join('\n') +
        info.stdout.join('\n')
      );
      err.info = info;
    }
    callback(err, info);
  });
};

helper.ccm.remove = function (callback) {
  this.exec(['remove'], callback);
};

/**
 * Reads the logs to see if the cql protocol is up
 * @param callback
 */
helper.ccm.waitForUp = function (callback) {
  let started = false;
  let retryCount = 0;
  const self = this;
  whilst(function () {
    return !started && retryCount < 60;
  }, function iterator (next) {
    helper.trace('Waiting for node1 to be UP');
    self.exec(['node1', 'showlog'], function (err, info) {
      if (err) {
        return next(err);
      }
      const regex = /Starting listening for CQL clients/mi;
      started = regex.test(info.stdout.join(''));
      retryCount++;
      if (!started) {
        //wait 1 sec between retries
        return setTimeout(next, 1000);
      }
      return next();
    });
  }, callback);
};

/**
 * Gets the path of the ccm
 * @param subPath
 */
helper.ccm.getPath = function (subPath) {
  let ccmPath = process.env.CCM_PATH;
  if (!ccmPath) {
    ccmPath = (process.platform === 'win32') ? process.env.HOMEPATH : process.env.HOME;
    ccmPath = path.join(ccmPath, 'workspace/tools/ccm');
  }
  return path.join(ccmPath, subPath);
};
/**
 * Conditionally executes func if testVersion is <= the current cassandra version.
 * @param {String} testVersion Minimum version of Cassandra needed.
 * @param {Function} func The function to conditionally execute.
 * @param {Array} args the arguments to apply to the function.
 */
function executeIfVersion (testVersion, func, args) {
  if (helper.versionCompare(helper.getDseVersion(), testVersion)) {
    func.apply(this, args);
  }
}

helper.ads._execute = function(processName, params, cb) {
  const originalProcessName = processName;
  if (process.platform.indexOf('win') === 0) {
    params = ['/c', processName].concat(params);
    processName = 'cmd.exe';
  }
  helper.trace('Executing: ' + processName + ' ' + params.join(" "));

  // If process hasn't completed in 10 seconds.
  let timeout = undefined;
  if(cb) {
    timeout = setTimeout(function() {
      cb("Timed out while waiting for " + processName + " to complete.");
    }, 10000);
  }

  const p = spawn(processName, params, {env:{KRB5_CONFIG: this.getKrb5ConfigPath()}});
  p.stdout.setEncoding('utf8');
  p.stderr.setEncoding('utf8');
  p.stdout.on('data', function (data) {
    helper.trace("%s_out> %s", originalProcessName, data);
  });

  p.stderr.on('data', function (data) {
    helper.trace("%s_err> %s", originalProcessName, data);
  });

  p.on('close', function (code) {
    helper.trace("%s exited with code %d", originalProcessName, code);
    if(cb) {
      clearTimeout(timeout);
      if (code === 0) {
        cb();
      } else {
        cb(Error("Process exited with non-zero exit code: " + code));
      }
    }
  });

  return p;
};

/**
 * Invokes a klist to list the current registered tickets and their expiration if trace is enabled.
 *
 * This is really only useful for debugging.
 *
 * @param {Function} cb Callback to invoke on completion.
 */
helper.ads.listTickets = function(cb) {
  this._execute('klist', [], cb);
};

/**
 * Acquires a ticket for the given username and its principal.
 * @param {String} username Username to acquire ticket for (i.e. cassandra).
 * @param {String} principal Principal to acquire ticket for (i.e. cassandra@DATASTAX.COM).
 * @param {Function} cb Callback to invoke on completion.
 */
helper.ads.acquireTicket = function(username, principal, cb) {
  const keytab = this.getKeytabPath(username);

  // Use ktutil on windows, kinit otherwise.
  const processName = 'kinit';
  const params = ['-t', keytab, '-k', principal];
  if (process.platform.indexOf('win') === 0) {
    // Not really sure what to do here yet...
  }
  this._execute(processName, params, cb);
};

/**
 * Destroys all tickets for the given principal.
 * @param {String} principal Principal for whom its tickets will be destroyed (i.e. dse/127.0.0.1@DATASTAX.COM).
 * @param {Function} cb Callback to invoke on completion.
 */
helper.ads.destroyTicket = function(principal, cb) {
  if (typeof principal === 'function') {
    //noinspection JSValidateTypes
    cb = principal;
    principal = null;
  }

  // Use ktutil on windows, kdestroy otherwise.
  const processName = 'kdestroy';
  const params = [];
  if (process.platform.indexOf('win') === 0) {
    // Not really sure what to do here yet...
  }
  this._execute(processName, params, cb);
};

/**
 * Stops the server process.
 * @param {Function} cb Callback to invoke when server stopped or with an error.
 */
helper.ads.stop = function(cb) {
  if(this.process !== undefined) {
    if(this.process.exitCode) {
      helper.trace("Server already stopped with exit code %d.", this.process.exitCode);
      cb();
    } else {
      this.process.on('close', function () {
        cb();
      });
      this.process.on('error', cb);
      this.process.kill('SIGINT');
    }
  } else {
    cb(Error("Process is not defined."));
  }
};

/**
 * Gets the path of the embedded-ads jar.  Resolved from ADS_JAR environment variable or $HOME/embedded-ads.jar.
 */
helper.ads.getJar = function () {
  let adsJar = process.env.ADS_JAR;
  if (!adsJar) {
    helper.trace("ADS_JAR environment variable not set, using $HOME/embedded-ads.jar");
    adsJar = (process.platform === 'win32') ? process.env.HOMEPATH : process.env.HOME;
    adsJar = path.join(adsJar, 'embedded-ads.jar');
  }
  helper.trace("Using %s for embedded ADS server.", adsJar);
  return adsJar;
};

/**
 * Returns the file path to the keytab for the given user.
 * @param {String} username User to resolve keytab for.
 */
helper.ads.getKeytabPath = function(username) {
  return path.join(this.dir, username + ".keytab");
};

/**
 * Returns the file path to the krb5.conf file generated by ads.
 */
helper.ads.getKrb5ConfigPath = function() {
  return path.join(this.dir, 'krb5.conf');
};

function series(arr, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter must be an Array');
  }
  callback = callback || noop;
  let index = 0;
  let sync;
  next();
  function next(err, result) {
    if (err) {
      return callback(err);
    }
    if (index === arr.length) {
      return callback(null, result);
    }
    if (sync) {
      return process.nextTick(function () {
        //noinspection JSUnusedAssignment
        sync = true;
        arr[index++](next);
        sync = false;
      });
    }
    sync = true;
    arr[index++](next);
    sync = false;
  }
}

/**
 * @param {Number} count
 * @param {Function} iteratorFunction
 * @param {Function} callback
 */
function timesSeries(count, iteratorFunction, callback) {
  count = +count;
  if (isNaN(count) || count < 1) {
    return callback();
  }
  let index = 1;
  let sync;
  iteratorFunction(0, next);
  if (sync === undefined) {
    sync = false;
  }
  function next(err) {
    if (err) {
      return callback(err);
    }
    if (index === count) {
      return callback();
    }
    if (sync === undefined) {
      sync = true;
    }
    const i = index++;
    if (sync) {
      //Prevent "Maximum call stack size exceeded"
      return process.nextTick(function () {
        iteratorFunction(i, next);
      });
    }
    //do a sync call as the callback is going to call on a future tick
    iteratorFunction(i, next);
  }
}

/**
 * @param {Array} arr
 * @param {Function} fn
 * @param {Function} [callback]
 */
function eachSeries(arr, fn, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter is not an Array');
  }
  callback = callback || noop;
  const length = arr.length;
  if (length === 0) {
    return callback();
  }
  let sync;
  let index = 1;
  fn(arr[0], next);
  if (sync === undefined) {
    sync = false;
  }

  function next(err) {
    if (err) {
      return callback(err);
    }
    if (index >= length) {
      return callback();
    }
    if (sync === undefined) {
      sync = true;
    }
    if (sync) {
      return process.nextTick(function () {
        fn(arr[index++], next);
      });
    }
    fn(arr[index++], next);
  }
}

/**
 * @param {Function} condition
 * @param {Function} fn
 * @param {Function} callback
 */
function whilst(condition, fn, callback) {
  let sync = 0;
  next();
  function next(err) {
    if (err) {
      return callback(err);
    }
    if (!condition()) {
      return callback();
    }
    if (sync === 0) {
      sync = 1;
      fn(function (err) {
        if (sync === 1) {
          //sync function
          sync = 4;
        }
        next(err);
      });
      if (sync === 1) {
        //async function
        sync = 2;
      }
      return;
    }
    if (sync === 4) {
      //Prevent "Maximum call stack size exceeded"
      return process.nextTick(function () {
        fn(next);
      });
    }
    //do a sync call as the callback is going to call on a future tick
    fn(next);
  }
}

helper.series = series;
helper.eachSeries = eachSeries;
helper.timesSeries = timesSeries;
helper.whilst = whilst;

module.exports = helper;
