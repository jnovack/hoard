/*jshint esversion: 6 */

const fs = require("fs");
const expect = require('chai').expect;
const hoard = require("../hoard.js");
const assert = require('assert');

const unixTime = function() {
  return parseInt(new Date().getTime() / 1000);
};

const FILENAME = 'test/large.whisper';

describe('info()', function () {
    it('contains all keys', function () {
        hoard.info(FILENAME).then(function(header){
            expect(header).to.have.all.keys('maxRetention', 'xFilesFactor', 'archives');;
        });
    });

    it('contains 2 archives', function () {
        hoard.info(FILENAME).then(function(header){
            expect(header.archives.length).to.equal(2);
        });
    });

    it('retention = 3 years', function () {
        hoard.info(FILENAME).then(function(header){
            expect(header.maxRetention).to.equal(94608000);
        });
    });

    it('xFilesFactor = 0.5', function () {
        hoard.info(FILENAME).then(function(header){
            expect(header.xFilesFactor).to.equal(0.5);
        });
    });

    describe('archive 1', function () {
        it('retention = 1 year', function () {
            hoard.info(FILENAME).then(function(header){
                expect(header.archives[0].retention).to.equal(31536000);
            });
        });

        it('secondsPerPoint = 3600', function () {
            hoard.info(FILENAME).then(function(header){
                expect(header.archives[0].secondsPerPoint).to.equal(3600);
            });
        });

        it('points = 8760', function () {
            hoard.info(FILENAME).then(function(header){
                expect(header.archives[0].points).to.equal(8760);
            });
        });

        it('size = 105120', function () {
            hoard.info(FILENAME).then(function(header){
                expect(header.archives[0].size).to.equal(105120);
            });
        });

        it('offset = 40', function () {
            hoard.info(FILENAME).then(function(header){
                expect(header.archives[0].offset).to.equal(40);
            });
        });
    });

    describe('archive 2', function () {
        it('retention = 3 years', function () {
            hoard.info(FILENAME).then(function(header){
                expect(header.archives[1].retention).to.equal(94608000);
            });
        });

        it('secondsPerPoint = 86400', function () {
            hoard.info(FILENAME).then(function(header){
                expect(header.archives[1].secondsPerPoint).to.equal(86400);
            });
        });

        it('points = 1095', function () {
            hoard.info(FILENAME).then(function(header){
                expect(header.archives[1].points).to.equal(1095);
            });
        });

        it('size = 13140', function () {
            hoard.info(FILENAME).then(function(header){
                expect(header.archives[1].size).to.equal(13140);
            });
        });

        it('offset = 105160', function () {
            hoard.info(FILENAME).then(function(header){
                expect(header.archives[1].offset).to.equal(105160);
            });
        });
    });
});
/*

// Tests against Python generated Whisper data file

process.exit();
var called, fromTime, toTime;
called = false;
fromTime = 1311161605;
toTime = 1311179605;
hoard.fetch(FILENAME, fromTime, toTime, function(err, timeInfo, values) {
  var v;
  if (err) {
    throw err;
  }
  called = true;
  assert.equal(1311163200, timeInfo[0]);
  assert.equal(1311181200, timeInfo[1]);
  assert.equal(3600, timeInfo[2]);
  v = [2048, 4546, 794, 805, 4718];
  assert.length(values, v.length);
  return assert.eql(v, values);
});



// Test Create
filename = 'test/testcreate.hoard';
if (fs.existsSync(filename)) {
  fs.unlinkSync(filename);
}
hoard.create(filename, [[1, 60], [10, 600]], 0.5, function(err) {
  var hoardFile, whisperFile;
  if (err) {
    throw err;
  }
  console.log(filename);
  hoardFile = fs.statSync(filename);
  whisperFile = fs.statSync('test/testcreate.whisper');
  console.log(hoardFile.size, whisperFile.size);
  assert.equal(whisperFile.size, hoardFile.size, "File lengths must match");
  return hoard.info(filename, function(err, header) {
    var archive;
    assert.equal(6000, header.maxRetention);
    assert.equal(0.5, header.xFilesFactor);
    assert.equal(2, header.archives.length);
    archive = header.archives[0];
    assert.equal(60, archive.retention);
    assert.equal(1, archive.secondsPerPoint);
    assert.equal(60, archive.points);
    assert.equal(720, archive.size);
    assert.equal(40, archive.offset);
    archive = header.archives[1];
    assert.equal(6000, archive.retention);
    assert.equal(10, archive.secondsPerPoint);
    assert.equal(600, archive.points);
    assert.equal(7200, archive.size);
    assert.equal(760, archive.offset);
  });
});


// FIXME: Compare to real file, must mock creation timestamp in create()
//assert.eql whisperFile, hoardFile
// return beforeExit(function() {
//   return assert.ok(called, "Callback must return");
// });

var called, filename;
    called = false;
    filename = 'test/testupdate.hoard';
if (fs.existsSync(filename)) {
  fs.unlinkSync(filename);
}
hoard.create(filename, [[3600, 8760], [86400, 1095]], 0.5, function(err) {
  if (err) {
    throw err;
  }
  return hoard.update(filename, 1337, 1311169605, function(err) {
    if (err) {
      throw err;
    }
    return hoard.fetch(filename, 1311161605, 1311179605, function(err, timeInfo, values) {
      if (err) {
        throw err;
      }
      called = true;
      equal(1311163200, timeInfo[0]);
      equal(1311181200, timeInfo[1]);
      equal(3600, timeInfo[2]);
      assert.length(values, 5);
      return equal(1337, values[1]);
    });
  });
});


var tsData;

filename = 'test/testupdatemany.hoard';
if (fs.existsSync(filename)) {
  fs.unlinkSync(filename);
}
tsData = JSON.parse(fs.readFileSync('test/timeseriesdata.json', 'utf8'));
console.log(tsData[0]);
hoard.create(filename, [[3600, 8760], [86400, 1095]], 0.5, function(err) {
  if (err) {
    throw err;
  }
  return hoard.updateMany(filename, tsData, function(err) {
    var from, to;
    if (err) {
      throw err;
    }
    from = 1311277105;
    to = 1311295105;
    return hoard.fetch(filename, from, to, function(err, timeInfo, values) {
      if (err) {
        throw err;
      }
      called = true;
      equal(1311278400, timeInfo[0]);
      equal(1311296400, timeInfo[1]);
      equal(3600, timeInfo[2]);
      assert.length(values, 5);
      return assert.eql([1043, 3946, 1692, 899, 2912], values);
    });
  });
});
*/