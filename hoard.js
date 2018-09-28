/*jshint esversion: 6 */
var _, create, fetch, info, propagate, underscore, unixTime, update, updateMany, updateManyArchive;

const fs = require('fs');
const Buffer = require('buffer').Buffer;
const Binary = require('binary');
const async = require('./lib/async');
const pack = require('./lib/jspack').jspack;
const Put = require('put');

underscore = _ = require('./lib/underscore');

// Monkey patch since modulo operator is broken in JS
Number.prototype.mod = function(n) {
    return ((this % n) + n) % n;
};

const longFormat = "!L";
const longSize = pack.CalcLength(longFormat);

const floatFormat = "!f";
const floatSize = pack.CalcLength(floatFormat);

const timestampFormat = "!L";
const timestampSize = pack.CalcLength(timestampFormat);

const valueFormat = "!d";
const valueSize = pack.CalcLength(valueFormat);

const pointFormat = "!Ld";
const pointSize = pack.CalcLength(pointFormat);

const metadataFormat = "!2LfL";
const metadataSize = pack.CalcLength(metadataFormat);

const archiveInfoFormat = "!3L";
const archiveInfoSize = pack.CalcLength(archiveInfoFormat);

unixTime = function() {
  return parseInt(new Date().getTime() / 1000);
};

create = (filename, archives, xFilesFactor) => {
    return (new Promise(function(resolve, reject) {
        var a, archive, archiveOffset, buffer, encodeFloat, headerSize, j, len, oldest, points, secondsPerPoint;
        // FIXME: Check parameters
        // FIXME: Check that values are correctly formatted
        archives.sort(function(a, b) {
            return a[0] - b[0];
        });
        if (fs.existsSync(filename)) {
            reject('File ' + filename + ' already exists');
        }
        oldest = ((function() {
            var j, len, results1;
            results1 = [];
            for (j = 0, len = archives.length; j < len; j++) {
                a = archives[j];
                results1.push(a[0] * a[1]);
            }
            return results1;
        })()).sort().reverse()[0];
        encodeFloat = function(value) {
            var buffer;
            // Dirty hack.
            // Using 'buffer_ieee754' from node 0.5.x
            // as no libraries had a working IEEE754 encoder
            buffer = new Buffer(4);
            require('./lib/buffer_ieee754.js').writeIEEE754(buffer, 0.5, 0, 'big', 23, 4);
            return buffer;
        };
        buffer = Put().word32be(unixTime()).word32be(oldest).put(encodeFloat(xFilesFactor)).word32be(archives.length); // last update // max retention
        headerSize = metadataSize + (archiveInfoSize * archives.length);
        archiveOffset = headerSize;
        for (j = 0, len = archives.length; j < len; j++) {
            archive = archives[j];
            secondsPerPoint = archive[0];
            points = archive[1];
            buffer.word32be(archiveOffset);
            buffer.word32be(secondsPerPoint);
            buffer.word32be(points);
            archiveOffset += points * pointSize;
        }
        // Pad archive data itself with zeroes
        buffer.pad(archiveOffset - headerSize);
        // FIXME: Check file lock?
        // FIXME: fsync this?
        return fs.writeFile(filename, buffer.buffer(), 'binary', () => { resolve(); } );
    }));
};

propagate = function(fd, timestamp, xff, higher, lower, cb) {
  var lowerIntervalEnd, lowerIntervalStart, packedPoint, parseSeries;
  lowerIntervalStart = timestamp - timestamp.mod(lower.secondsPerPoint);
  lowerIntervalEnd = lowerIntervalStart + lower.secondsPerPoint;
  packedPoint = new Buffer(pointSize);
  fs.read(fd, packedPoint, 0, pointSize, higher.offset, function(err, written, buffer) {
    var byteDistance, firstSeriesSize, higherBaseInterval, higherBaseValue, higherEnd, higherFirstOffset, higherLastOffset, higherPoints, higherSize, pointDistance, relativeFirstOffset, relativeLastOffset, secondSeriesSize, seriesSize, seriesString, timeDistance;
    if (err) {
      cb(err);
    }
    [higherBaseInterval, higherBaseValue] = pack.Unpack(pointFormat, packedPoint);
    if (higherBaseInterval === 0) {
      higherFirstOffset = higher.offset;
    } else {
      timeDistance = lowerIntervalStart - higherBaseInterval;
      pointDistance = timeDistance / higher.secondsPerPoint;
      byteDistance = pointDistance * pointSize;
      higherFirstOffset = higher.offset + byteDistance.mod(higher.size);
    }
    higherPoints = lower.secondsPerPoint / higher.secondsPerPoint;
    higherSize = higherPoints * pointSize;
    relativeFirstOffset = higherFirstOffset - higher.offset;
    relativeLastOffset = (relativeFirstOffset + higherSize).mod(higher.size);
    higherLastOffset = relativeLastOffset + higher.offset;
    if (higherFirstOffset < higherLastOffset) {
      // We don't wrap the archive
      seriesSize = higherLastOffset - higherFirstOffset;
      seriesString = new Buffer(seriesSize);
      return fs.read(fd, seriesString, 0, seriesSize, higherFirstOffset, function(err, written, buffer) {
        return parseSeries(seriesString);
      });
    } else {
      // We do wrap the archive
      higherEnd = higher.offset + higher.size;
      firstSeriesSize = higherEnd - higherFirstOffset;
      secondSeriesSize = higherLastOffset - higher.offset;
      seriesString = new Buffer(firstSeriesSize + secondSeriesSize);
      return fs.read(fd, seriesString, 0, firstSeriesSize, higherFirstOffset, function(err, written, buffer) {
        var ret;
        if (err) {
          cb(err);
        }
        if (secondSeriesSize > 0) {
          return fs.read(fd, seriesString, firstSeriesSize, secondSeriesSize, higher.offset, function(err, written, buffer) {
            if (err) {
              cb(err);
            }
            return parseSeries(seriesString);
          });
        } else {
          ret = new Buffer(firstSeriesSize);
          seriesString.copy(ret, 0, 0, firstSeriesSize);
          return parseSeries(ret);
        }
      });
    }
  });
  return parseSeries = function(seriesString) {
    var aggregateValue, byteOrder, currentInterval, f, i, j, knownPercent, knownValues, myPackedPoint, neighborValues, pointTime, pointTypes, points, ref, seriesFormat, step, sum, unpackedSeries, v;
    // Now we unpack the series data we just read
    [byteOrder, pointTypes] = [pointFormat[0], pointFormat.slice(1)];
    points = seriesString.length / pointSize;
    seriesFormat = byteOrder + ((function() {
      var j, ref, results1;
      results1 = [];
      for (f = j = 0, ref = points; (0 <= ref ? j < ref : j > ref); f = 0 <= ref ? ++j : --j) {
        results1.push(pointTypes);
      }
      return results1;
    })()).join("");
    unpackedSeries = pack.Unpack(seriesFormat, seriesString, 0);
    // And finally we construct a list of values
    neighborValues = (function() {
      var j, ref, results1;
      results1 = [];
      for (f = j = 0, ref = points; (0 <= ref ? j < ref : j > ref); f = 0 <= ref ? ++j : --j) {
        results1.push(null);
      }
      return results1;
    })();
    currentInterval = lowerIntervalStart;
    step = higher.secondsPerPoint;
    for (i = j = 0, ref = unpackedSeries.length; j < ref; i = j += 2) {
      pointTime = unpackedSeries[i];
      if (pointTime === currentInterval) {
        neighborValues[i / 2] = unpackedSeries[i + 1];
      }
      currentInterval += step;
    }
    // Propagate aggregateValue to propagate from neighborValues if we have enough known points
    knownValues = (function() {
      var k, len, results1;
      results1 = [];
      for (k = 0, len = neighborValues.length; k < len; k++) {
        v = neighborValues[k];
        if (v !== null) {
          results1.push(v);
        }
      }
      return results1;
    })();
    if (knownValues.length === 0) {
      cb(null, false);
      return;
    }
    sum = function(list) {
      var k, len, s, x;
      s = 0;
      for (k = 0, len = list.length; k < len; k++) {
        x = list[k];
        s += x;
      }
      return s;
    };
    knownPercent = knownValues.length / neighborValues.length;
    if (knownPercent >= xff) {
      // We have enough data to propagate a value!
      aggregateValue = sum(knownValues) / knownValues.length; // TODO: Another CF besides average?
      myPackedPoint = pack.Pack(pointFormat, [lowerIntervalStart, aggregateValue]);
      // !!!!!!!!!!!!!!!!!
      packedPoint = new Buffer(pointSize);
      return fs.read(fd, packedPoint, 0, pointSize, lower.offset, function(err) {
        var byteDistance, lowerBaseInterval, lowerBaseValue, mypp, offset, pointDistance, timeDistance;
        [lowerBaseInterval, lowerBaseValue] = pack.Unpack(pointFormat, packedPoint);
        if (lowerBaseInterval === 0) {
          // First propagated update to this lower archive
          offset = lower.offset;
        } else {
          // Not our first propagated update to this lower archive
          timeDistance = lowerIntervalStart - lowerBaseInterval;
          pointDistance = timeDistance / lower.secondsPerPoint;
          byteDistance = pointDistance * pointSize;
          offset = lower.offset + byteDistance.mod(lower.size);
        }
        mypp = new Buffer(myPackedPoint);
        return fs.write(fd, mypp, 0, pointSize, offset, function(err) {
          return cb(null, true);
        });
      });
    } else {
      return cb(null, false);
    }
  };
};

update = function(filename, value, timestamp) {
    return (new Promise(function(resolve, reject) {
        // FIXME: Check file lock?
        // FIXME: Don't use info(), re-use fd between internal functions
        info(filename).then( (header) => {
            var archive, diff, i, j, lowerArchives, now, ref;
            now = unixTime();
            diff = now - timestamp;
            if (!(diff < header.maxRetention && diff >= 0)) {
                reject(diff + ' timestamp not covered by ' + now + 'any archives in this database ' + header.maxRetention);
                return;
            }
            // Find the highest-precision archive that covers timestamp
            for (i = j = 0, ref = header.archives.length; (0 <= ref ? j < ref : j > ref); i = 0 <= ref ? ++j : --j) {
                archive = header.archives[i];
                if (archive.retention < diff) {
                    continue;
                }
                // We'll pass on the update to these lower precision archives later
                lowerArchives = header.archives.slice(i + 1);
                break;
            }
            return fs.open(filename, 'r+', function(err, fd) {
                var myInterval, myPackedPoint, packedPoint, propagateLowerArchives;
                if (err) {
                    reject(err);
                }
                // First we update the highest-precision archive
                myInterval = timestamp - timestamp.mod(archive.secondsPerPoint);
                myPackedPoint = new Buffer(pack.Pack(pointFormat, [myInterval, value]));
                packedPoint = new Buffer(pointSize);
                fs.read(fd, packedPoint, 0, pointSize, archive.offset, function(err, bytesRead, buffer) {
                    var baseInterval, baseValue, byteDistance, myOffset, pointDistance, timeDistance;
                    if (err) {
                        reject(err);
                    }
                    [baseInterval, baseValue] = pack.Unpack(pointFormat, packedPoint);
                    if (baseInterval === 0) {
                        // This file's first update
                        return fs.write(fd, myPackedPoint, 0, pointSize, archive.offset, function(err, written, buffer) {
                            if (err) {
                                reject(err);
                            }
                            [baseInterval, baseValue] = [myInterval, value];
                            return propagateLowerArchives();
                        });
                    } else {
                        // File has been updated before
                        timeDistance = myInterval - baseInterval;
                        pointDistance = timeDistance / archive.secondsPerPoint;
                        byteDistance = pointDistance * pointSize;
                        myOffset = archive.offset + byteDistance.mod(archive.size);
                        return fs.write(fd, myPackedPoint, 0, pointSize, myOffset, function(err, written, buffer) {
                            if (err) {
                                reject(err);
                            }
                            return propagateLowerArchives();
                        });
                    }
                });
                resolve(propagateLowerArchives = function() {
                    // Propagate the update to lower-precision archives
                    //higher = archive
                    //for lower in lowerArchives:
                    //    if not __propagate(fd, myInterval, header.xFilesFactor, higher, lower):
                    //        break
                    //    higher = lower

                    //__changeLastUpdate(fh)

                    // FIXME: Also fsync here?
                    return fs.closeSync(fd);
                });
            });
        }).catch( (err) => { reject(err); });
    }));
};

updateMany = function(filename, points) {
    return (new Promise(function(resolve, reject) {
        points.sort(function(a, b) {
            return a[0] - b[0];
        }).reverse();
        // FIXME: Check lock
        return info(filename, function(err, header) {
            if (err) {
                cb(err);
            }
            return fs.open(filename, 'r+', function(err, fd) {
                var age, archives, currentArchive, currentArchiveIndex, currentPoints, j, len, now, point, updateArchiveCalls;
                now = unixTime();
                archives = header.archives;
                currentArchiveIndex = 0;
                currentArchive = header.archives[currentArchiveIndex];
                currentPoints = [];
                updateArchiveCalls = [];
                for (j = 0, len = points.length; j < len; j++) {
                    point = points[j];
                    age = now - point[0];
                    while (currentArchive.retention < age) { // We can't fit any more points in this archive
                    if (currentPoints) {
                        // Commit all the points we've found that it can fit
                        currentPoints.reverse(); // Put points in chronological order
                        (function(header, currentArchive, currentPoints) {
                            var f;
                            f = function(cb) {
                                return updateManyArchive(fd, header, currentArchive, currentPoints, cb);
                            };
                            return updateArchiveCalls.push(f);
                        })(header, currentArchive, currentPoints);
                        currentPoints = [];
                    }
                    if (currentArchiveIndex < (archives.length - 1)) {
                        currentArchiveIndex++;
                        currentArchive = archives[currentArchiveIndex];
                    } else {
                        // Last archive
                        currentArchive = null;
                        break;
                    }
                }
                if (!currentArchive) {
                    break; // Drop remaining points that don't fit in the database
                }
                currentPoints.push(point);
            }
            return async.series(updateArchiveCalls, function(err, results) {
                if (err) {
                    throw err;
                }
                if (currentArchive && currentPoints.length > 0) {
                    // Don't forget to commit after we've checked all the archives
                    currentPoints.reverse();
                    return updateManyArchive(fd, header, currentArchive, currentPoints, function(err) {
                        if (err) {
                            throw err;
                        }
                        return fs.close(fd, cb);
                    });
                } else {
                    return fs.close(fd, cb);
                }
            });
        });
    });
}));

};

// FIXME: touch last update
// FIXME: fsync here?
// FIXME: close fd fh.close()
//cb(null)
updateManyArchive = function(fd, header, archive, points, cb) {
  var alignedPoints, ap, currentString, interval, j, k, len, len1, numberOfPoints, p, packedBasePoint, packedStrings, previousInterval, startInterval, step, timestamp, value;
  step = archive.secondsPerPoint;
  alignedPoints = [];
  for (j = 0, len = points.length; j < len; j++) {
    p = points[j];
    [timestamp, value] = p;
    alignedPoints.push([timestamp - timestamp.mod(step), value]);
  }
  // Create a packed string for each contiguous sequence of points
  packedStrings = [];
  previousInterval = null;
  currentString = [];
  for (k = 0, len1 = alignedPoints.length; k < len1; k++) {
    ap = alignedPoints[k];
    [interval, value] = ap;
    if (!previousInterval || (interval === previousInterval + step)) {
      currentString.concat(pack.Pack(pointFormat, [interval, value]));
      previousInterval = interval;
    } else {
      numberOfPoints = currentString.length / pointSize;
      startInterval = previousInterval - (step * (numberOfPoints - 1));
      packedStrings.push([startInterval, new Buffer(currentString)]);
      currentString = pack.Pack(pointFormat, [interval, value]);
      previousInterval = interval;
    }
  }
  if (currentString.length > 0) {
    numberOfPoints = currentString.length / pointSize;
    startInterval = previousInterval - (step * (numberOfPoints - 1));
    packedStrings.push([startInterval, new Buffer(currentString, 'binary')]);
  }
  // Read base point and determine where our writes will start
  packedBasePoint = new Buffer(pointSize);
  return fs.read(fd, packedBasePoint, 0, pointSize, archive.offset, function(err) {
    var baseInterval, baseValue, propagateLowerArchives, writePackedString;
    if (err) {
      cb(err);
    }
    [baseInterval, baseValue] = pack.Unpack(pointFormat, packedBasePoint);
    if (baseInterval === 0) {
      // This file's first update
      // Use our first string as the base, so we start at the start
      baseInterval = packedStrings[0][0];
    }
    // Write all of our packed strings in locations determined by the baseInterval
    writePackedString = function(ps, callback) {
      var archiveEnd, byteDistance, bytesBeyond, myOffset, packedString, pointDistance, timeDistance;
      [interval, packedString] = ps;
      timeDistance = interval - baseInterval;
      pointDistance = timeDistance / step;
      byteDistance = pointDistance * pointSize;
      myOffset = archive.offset + byteDistance.mod(archive.size);
      archiveEnd = archive.offset + archive.size;
      bytesBeyond = (myOffset + packedString.length) - archiveEnd;
      if (bytesBeyond > 0) {
        return fs.write(fd, packedString, 0, packedString.length - bytesBeyond, myOffset, function(err) {
          if (err) {
            cb(err);
          }
          assert.equal(archiveEnd, myOffset + packedString.length - bytesBeyond);
          //assert fh.tell() == archiveEnd, "archiveEnd=%d fh.tell=%d bytesBeyond=%d len(packedString)=%d" % (archiveEnd,fh.tell(),bytesBeyond,len(packedString))
          // Safe because it can't exceed the archive (retention checking logic above)
          return fs.write(fd, packedString, packedString.length - bytesBeyond, bytesBeyond, archive.offset, function(err) {
            if (err) {
              cb(err);
            }
            return callback();
          });
        });
      } else {
        return fs.write(fd, packedString, 0, packedString.length, myOffset, function(err) {
          return callback();
        });
      }
    };
    async.forEachSeries(packedStrings, writePackedString, function(err) {
      if (err) {
        throw err;
      }
      return propagateLowerArchives();
    });
    return propagateLowerArchives = function() {
      var arc, callPropagate, fit, higher, l, len2, len3, lower, lowerArchives, lowerIntervals, m, propagateCalls, uniqueLowerIntervals;
      // Now we propagate the updates to lower-precision archives
      higher = archive;
      lowerArchives = (function() {
        var l, len2, ref, results1;
        ref = header.archives;
        results1 = [];
        for (l = 0, len2 = ref.length; l < len2; l++) {
          arc = ref[l];
          if (arc.secondsPerPoint > archive.secondsPerPoint) {
            results1.push(arc);
          }
        }
        return results1;
      })();
      if (lowerArchives.length > 0) {
        // Collect a list of propagation calls to make
        // This is easier than doing async looping
        propagateCalls = [];
        for (l = 0, len2 = lowerArchives.length; l < len2; l++) {
          lower = lowerArchives[l];
          fit = function(i) {
            return i - i.mod(lower.secondsPerPoint);
          };
          lowerIntervals = (function() {
            var len3, m, results1;
            results1 = [];
            for (m = 0, len3 = alignedPoints.length; m < len3; m++) {
              p = alignedPoints[m];
              results1.push(fit(p[0]));
            }
            return results1;
          })();
          uniqueLowerIntervals = _.uniq(lowerIntervals);
          for (m = 0, len3 = uniqueLowerIntervals.length; m < len3; m++) {
            interval = uniqueLowerIntervals[m];
            propagateCalls.push({
              interval: interval,
              header: header,
              higher: higher,
              lower: lower
            });
          }
          higher = lower;
        }
        callPropagate = function(args, callback) {
          return propagate(fd, args.interval, args.header.xFilesFactor, args.higher, args.lower, function(err, result) {
            if (err) {
              cb(err);
            }
            return callback(err, result);
          });
        };
        return async.forEachSeries(propagateCalls, callPropagate, function(err, result) {
          if (err) {
            throw err;
          }
          return cb(null);
        });
      } else {
        return cb(null);
      }
    };
  });
};

info = path => {
    return (new Promise(function(resolve, reject) {
        // FIXME: Close this stream?
        // FIXME: Signal errors to callback?
        // FIXME: Stream parsing with node-binary dies
        // Looks like an issue, see their GitHub
        // Using fs.readFile() instead of read stream for now
        fs.readFile(path, function(err, data) {
            var archives, metadata;
            if (err) {
                reject(err);
            }
            archives = [];
            metadata = {};
            Binary.parse(data).word32bu('lastUpdate').word32bu('maxRetention').buffer('xff', 4).word32bu('archiveCount').tap(function(vars) { // Must decode separately since node-binary can't handle floats
                var index, j, ref, results1;
                metadata = vars;
                metadata.xff = pack.Unpack('!f', vars.xff, 0)[0];
                this.flush();
                results1 = [];
                for (index = j = 0, ref = metadata.archiveCount; (0 <= ref ? j < ref : j > ref); index = 0 <= ref ? ++j : --j) {
                    this.word32bu('offset').word32bu('secondsPerPoint').word32bu('points');
                    results1.push(this.tap(function(archive) {
                        this.flush();
                        archive.retention = archive.secondsPerPoint * archive.points;
                        archive.size = archive.points * pointSize;
                        return archives.push(archive);
                    }));
                }
                return results1;
            }).tap(function() {
                resolve({
                    maxRetention: metadata.maxRetention,
                    xFilesFactor: metadata.xff,
                    archives: archives
                });
            });
        });
    }));
};

// fetch = function(path, from, to, cb) {
fetch = (path, from, to) => {
    return (new Promise(function(resolve, reject) {
        info(path).then( header => {
            var archive, diff, fd, file, fromInterval, j, len, now, oldestTime, ref, toInterval, unpack;
            now = unixTime();
            oldestTime = now - header.maxRetention;
            if (from < oldestTime) {
                from = oldestTime;
            }
            if (to > now || to < from) {
                to = now;
            }
            if (!(from < to)) {
                reject(from + " is not less than " + to);
            }
            diff = now - from;
            fd = null;
            ref = header.archives;
            // Find closest archive to look in, that will contain our information
            for (j = 0, len = ref.length; j < len; j++) {
                archive = ref[j];
                if (archive.retention >= diff) {
                    break;
                }
            }
            fromInterval = parseInt(from - from.mod(archive.secondsPerPoint)) + archive.secondsPerPoint;
            toInterval = parseInt(to - to.mod(archive.secondsPerPoint)) + archive.secondsPerPoint;
            file = fs.createReadStream(path);
            Binary.stream(file).skip(archive.offset).word32bu('baseInterval').word32bu('baseValue').tap(function(vars) {
                var fromOffset, getOffset, n, points, step, timeInfo, toOffset, values;
                if (vars.baseInterval === 0) {
                    // Nothing has been written to this hoard
                    step = archive.secondsPerPoint;
                    points = (toInterval - fromInterval) / step;
                    timeInfo = [fromInterval, toInterval, step];
                    values = (function() {
                        var k, ref1, results1;
                        results1 = [];
                        for (n = k = 0, ref1 = points; (0 <= ref1 ? k < ref1 : k > ref1); n = 0 <= ref1 ? ++k : --k) {
                            results1.push(null);
                        }
                        return results1;
                    })();
                    resolve({meta: timeInfo, data: values});
                } else {
                    // We have data in this hoard, let's read it
                    getOffset = function(interval) {
                        var a, byteDistance, pointDistance, timeDistance;
                        timeDistance = interval - vars.baseInterval;
                        pointDistance = timeDistance / archive.secondsPerPoint;
                        byteDistance = pointDistance * pointSize;
                        a = archive.offset + byteDistance.mod(archive.size);
                        return a;
                    };
                    fromOffset = getOffset(fromInterval);
                    toOffset = getOffset(toInterval);
                    return fs.open(path, 'r', function(err, fd) {
                        var archiveEnd, seriesBuffer, size, size1, size2;
                        if (err) {
                            throw err;
                        }
                        if (fromOffset < toOffset) {
                            // We don't wrap around, can everything in a single read
                            size = toOffset - fromOffset;
                            seriesBuffer = new Buffer(size);
                            return fs.read(fd, seriesBuffer, 0, size, fromOffset, function(err, num) {
                                if (err) {
                                    reject(err);
                                }
                                return fs.close(fd, function(err) {
                                    if (err) {
                                        reject(err);
                                    }
                                    return unpack(seriesBuffer); // We have read it, go unpack!
                                });
                            });
                        } else {
                            // We wrap around the archive, we need two reads
                            archiveEnd = archive.offset + archive.size;
                            size1 = archiveEnd - fromOffset;
                            size2 = toOffset - archive.offset;
                            seriesBuffer = new Buffer(size1 + size2);
                            return fs.read(fd, seriesBuffer, 0, size1, fromOffset, function(err, num) {
                                if (err) {
                                    reject(err);
                                }
                                return fs.read(fd, seriesBuffer, size1, size2, archive.offset, function(err, num) {
                                    if (err) {
                                        reject(err);
                                    }
                                    unpack(seriesBuffer); // We have read it, go unpack!
                                    return fs.closeSync(fd);
                                });
                            });
                        }
                    });
                }
            });
            return unpack = function(seriesData) {
                var currentInterval, f, i, k, numPoints, pointTime, pointValue, ref1, seriesFormat, step, timeInfo, unpackedSeries, valueList;
                // Optmize this?
                numPoints = seriesData.length / pointSize;
                seriesFormat = "!" + ((function() {
                    var k, ref1, results1;
                    results1 = [];
                    for (f = k = 0, ref1 = numPoints; (0 <= ref1 ? k < ref1 : k > ref1); f = 0 <= ref1 ? ++k : --k) {
                        results1.push('Ld');
                    }
                    return results1;
                })()).join("");
                unpackedSeries = pack.Unpack(seriesFormat, seriesData);
                // Use buffer/pre-allocate?
                valueList = (function() {
                    var k, ref1, results1;
                    results1 = [];
                    for (f = k = 0, ref1 = numPoints; (0 <= ref1 ? k < ref1 : k > ref1); f = 0 <= ref1 ? ++k : --k) {
                        results1.push(null);
                    }
                    return results1;
                })();
                currentInterval = fromInterval;
                step = archive.secondsPerPoint;
                for (i = k = 0, ref1 = unpackedSeries.length; k < ref1; i = k += 2) {
                    pointTime = unpackedSeries[i];
                    if (pointTime === currentInterval) {
                        pointValue = unpackedSeries[i + 1];
                        valueList[i / 2] = pointValue;
                    }
                    currentInterval += step;
                }
                timeInfo = [fromInterval, toInterval, step];
                resolve({meta: timeInfo, data: valueList});
            };
        });
    }));
};

exports.create = create;

exports.update = update;

exports.updateMany = updateMany;

exports.info = info;

exports.fetch = fetch;
