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

    this.beforeAll( (done) => {
        hoard.info(FILENAME).then( ret => {
            header = ret;
            done();
        });
    });

    it('contains all keys', function () {
        expect(header).to.have.all.keys('maxRetention', 'xFilesFactor', 'archives');
    });

    it('contains 2 archives', function () {
        expect(header.archives.length).to.equal(2);
    });

    it('retention = 3 years', function () {
        expect(header.maxRetention).to.equal(94608000);
    });

    it('xFilesFactor = 0.5', function () {
        expect(header.xFilesFactor).to.equal(0.5);
    });

    describe('archive 1', function () {
        it('retention = 1 year', function () {
            expect(header.archives[0].retention).to.equal(31536000);
        });

        it('secondsPerPoint = 3600', function () {
            expect(header.archives[0].secondsPerPoint).to.equal(3600);
        });

        it('points = 8760', function () {
            expect(header.archives[0].points).to.equal(8760);
        });

        it('size = 105120', function () {
            expect(header.archives[0].size).to.equal(105120);
        });

        it('offset = 40', function () {
            expect(header.archives[0].offset).to.equal(40);
        });
    });

    describe('archive 2', function () {
        it('retention = 3 years', function () {
            expect(header.archives[1].retention).to.equal(94608000);
        });

        it('secondsPerPoint = 86400', function () {
            expect(header.archives[1].secondsPerPoint).to.equal(86400);
        });

        it('points = 1095', function () {
            expect(header.archives[1].points).to.equal(1095);
        });

        it('size = 13140', function () {
            expect(header.archives[1].size).to.equal(13140);
        });

        it('offset = 105160', function () {
            expect(header.archives[1].offset).to.equal(105160);
        });
    });
});


// 1532865600 // Set Dates Dynamically
const fromTime = 1532865550;
const toTime = 1532865650;
/*
// Currently failing because original whisper file is too old!
const values = [2048, 4546, 794, 805, 4718];
describe('fetch()', function () {

    hoard.fetch(FILENAME, fromTime, toTime).then( ret => {
        timeInfo = ret.meta;
        vals = ret.data;
        console.log(timeInfo);
    });
    it('test first entry', function () {
            expect(timeInfo[0]).to.equal(1311163200);
    });

    it('test second entry', function () {
            expect(timeInfo[1]).to.equal(1311181200);
    });

    it('test third entry', function () {
            expect(timeInfo[2]).to.equal(3600);
    });

    it('array lengths', function () {
            expect(vals).to.have.lengthOf(5);
    });

    it.skip('array equals', function () {
            expect(vals).to.equal(values);
    });
});
*/

// Test Create
filename = 'test/testcreate.hoard';
if (fs.existsSync(filename)) {
    fs.unlinkSync(filename);
}

describe('create()', function () {

    this.beforeAll( (done) => {
        hoard.create(filename, [[1, 60], [10, 600]], 0.5).then( () => {
            done();
        });
    });
    this.beforeAll( (done) => {
        hoard.info(filename).then( ret => {
            header = ret;
            done();
        });
    });

    it('has the right size', function () {
        hoardFile = fs.statSync(filename);
        expect(hoardFile.size).to.equal(7960);
    });

    it('contains all keys', function () {
        expect(header).to.have.all.keys('maxRetention', 'xFilesFactor', 'archives');
    });

    it('contains 2 archives', function () {
        expect(header.archives.length).to.equal(2);
    });

    it('retention = 3 years', function () {
        expect(header.maxRetention).to.equal(6000);
    });

    it('xFilesFactor = 0.5', function () {
        expect(header.xFilesFactor).to.equal(0.5);
    });


    describe('archive 1', function () {
        it('retention = 60 seconds', function () {
            expect(header.archives[0].retention).to.equal(60);
        });

        it('secondsPerPoint = 1', function () {
            expect(header.archives[0].secondsPerPoint).to.equal(1);
        });

        it('points = 60', function () {
            expect(header.archives[0].points).to.equal(60);
        });

        it('size = 720', function () {
            expect(header.archives[0].size).to.equal(720);
        });

        it('offset = 40', function () {
            expect(header.archives[0].offset).to.equal(40);
        });
    });

    describe('archive 2', function () {
        it('retention = 100 minutes', function () {
            expect(header.archives[1].retention).to.equal(6000);
        });

        it('secondsPerPoint = 10', function () {
            expect(header.archives[1].secondsPerPoint).to.equal(10);
        });

        it('points = 600', function () {
            expect(header.archives[1].points).to.equal(600);
        });

        it('size = 7200', function () {
            expect(header.archives[1].size).to.equal(7200);
        });

        it('offset = 760', function () {
            expect(header.archives[1].offset).to.equal(760);
        });
    });
});

// Test Update
describe('update()', function () {

    this.beforeAll( (done) => {
        filename = 'test/testupdate.hoard';
        if (fs.existsSync(filename)) {
            fs.unlinkSync(filename);
        }
        done();
    });

    this.beforeAll( (done) => {
        hoard.create(filename, [[3600, 8766]], 0.5).then( () => {
            done();
        });
    });
    this.beforeAll( (done) => {
        hoard.update(filename, 1337, 1532865600).then( () => {
            done();
        });
    });
    this.beforeAll( (done) => {
        hoard.fetch(filename, fromTime, toTime).then( ret => {
            timeInfo = ret.meta;
            vals = ret.data;
            done();
        });
    });
    this.beforeAll( (done) => {
        hoard.info(filename).then( ret => {
            header = ret;
            done();
        });
    });

    it('time start', function () {
        expect(timeInfo[0]).to.equal(1532865600);
    });

    it('time end', function () {
            expect(timeInfo[1]).to.equal(1532869200);
    });

    it('time interval', function () {
            expect(timeInfo[2]).to.equal(3600);
    });

    it('array lengths', function () {
            expect(vals).to.have.lengthOf(1);
    });

    it('array equals', function () {
            expect(vals[0]).to.equal(1337);
    });

    describe('archive 1', function () {
        it('retention = 1 year', function () {
            expect(header.archives[0].retention).to.equal(31557600);
        });

        it('secondsPerPoint = 1', function () {
            expect(header.archives[0].secondsPerPoint).to.equal(3600);
        });

        it('points = 8766', function () {
            expect(header.archives[0].points).to.equal(8766);
        });

        it('size = 105192', function () {
            expect(header.archives[0].size).to.equal(105192);
        });

        it('offset = 28', function () {
            expect(header.archives[0].offset).to.equal(28);
        });
    });
});

// Offset by 220924800
filename = 'test/testupdatemany.hoard';
if (fs.existsSync(filename)) {
  fs.unlinkSync(filename);
}

describe('updateMany()', function () {

    this.beforeAll( (done) => {
        filename = 'test/testupdatemany.hoard';
        if (fs.existsSync(filename)) {
            fs.unlinkSync(filename);
        }
        tsData = JSON.parse(fs.readFileSync('test/timeseriesdata.json', 'utf8'));
        done();
    });
    this.beforeAll( (done) => {
        hoard.create(filename, [[3600, 87600]/*, [86400, 3650]*/], 0.5).then( () => {
            done();
        });
    });
    this.beforeAll( (done) => {
        hoard.updateMany(filename, tsData).then( () => {
            done();
        });
    });
    this.beforeAll( (done) => {
        from = 1532205000; // Sat, 21 Jul 2018 16:30:00 EDT (-0400)
        to = 1532223000;   // Sat, 21 Jul 2018 21:30:00 EDT (-0400)
        hoard.fetch(filename, fromTime, toTime).then( ret => {
            timeInfo = ret.meta;
            vals = ret.data;
            done();
        });
    });
    this.beforeAll( (done) => {
        hoard.info(filename).then( ret => {
            header = ret;
            done();
        });
    });

    /*
        [1532203705,714]
        [1532205505,1043]
        [1532207305,2381]
        [1532209105,3946]
        [1532210905,770]
        [1532212705,1692]
        [1532214505,3376]
        [1532216305,899]
        [1532218105,1468]
        [1532219905,2912]
        [1532221705,4632]
    */

    it('time start', function () { // Sat, 21 Jul 2018 16:30:00 EDT (-0400)
        expect(timeInfo[0]).to.equal(1532203200); // 1311278400 + 220924800;
    });
    it('time end', function () { // Sun, 22 Jul 2018 00:50:00 EDT (-0400)
        expect(timeInfo[1]).to.equal(1532235000); // 1311296400 + 220924800;
    });
    it('time interval', function () {
        expect(timeInfo[2]).to.equal(3600);
    });
    it('header offset', function () {
        expect(header.archives[0].offset).to.equal(28);
    });
    it('values.length', function () {
        expect(vals.length).to.equal(5);
    });
    it('offset = 28', function () {
        expect(vals).to.equal([1043, 3946, 1692, 899, 2912]);
    });
    //   equal(1311278400 + 220924800, timeInfo[0]);
    //   equal(1311296400 + 220924800, timeInfo[1]);
    //   equal(3600, timeInfo[2]);
    //   assert.length(values, 5);
    //   assert.eql([1043, 3946, 1692, 899, 2912], values);
});