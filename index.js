var fs 	 		= require("fs");
var multiparty 	= require('multiparty');
var _ 			= require('underscore');
var createHash 	= require('sha.js');
var sha256 		= createHash('sha256');
var UUID 		= require('node-uuid');
var Helper		= require('./helper');


function Storage(core, options){
	var self 	= this;
	self.db 	= options.db;
	self.GridStore = options.GridStore;
	self._useChecksumHash = !!options.useChecksumHash;
	self._useContentHashAsName = !!options.useContentHashAsName;
}

Storage.prototype.useContentHashAsName = function( useContentHashAsName ){
	this._useContentHashAsName = !!useContentHashAsName;
}

Storage.prototype.store = function(req, validateFn, callbackFn, db){
	var self = this;

	self.parseReq(req, function(err, fields, files){
		if (err)
			return callbackFn(err);

		validateFn(fields, files, function(err, metadata, fileNames){
			if (err)
				return callbackFn(err);

			self.storeFiles(db || self.db, files, fileNames, metadata, function(err, result){
				if (err)
					return callbackFn(err);

				callbackFn(null, result)
			});
		})
	})
};

Storage.prototype.parseReq = function(req, callback){
	var form = new multiparty.Form();
	form.parse(req, function(err, fields, files) {
		callback(err, fields, files);
	});
};

Storage.prototype.storeFiles = function(db, files, fileNameList, metaDataList, callback){
	var self 	= this;

	var result = {}, totalCount = 0, count = 0, error = false, fileName, metadata;
	_.each(files, function(fileArray, key){
		totalCount += fileArray.length;
	});

	if ( !totalCount )
		return callback({error:'No file uploaded.'});

	_.each(files, function(fileArray, key){
		key = key.split('[')[0];
		_.each(fileArray, function(file, index){
			fileName = null, metadata = {};
			if ( fileNameList && _.isArray(fileNameList[key]) && fileNameList[key][index] ) {
				fileName = fileNameList[key][index];
			};

			if ( metaDataList && _.isArray(metaDataList[key]) && metaDataList[key][index] ) {
				metadata = metaDataList[key][index];
			};
			storeFile(file, fileName, metadata, key, index);
		});
	});

	function storeFile(file, fileName, metadata, key, index){
		self.storeFile(db, file, fileName, metadata, function(err, info){
			if (!result[key]) {
				result[key] = [];
			};
			if (err){
				result[key].push({error: err, index: index});
				error 	= err;
			}else{
				result[key].push({info:info, index:index});
			}
			count++;

			doCallback();
		});
	}

	function doCallback(){
		if ( count >= totalCount ) {
			if (error) {
				callback( {error: 'Error in saving some files.', result: result} );
			}else{
				callback( null, result );
			}
		};
	}
};

Storage.prototype.createHash = function( str ){
	if (this._useChecksumHash)
		return this.checksum(str.toString());

	return sha256.update( str.toString() ).digest('hex');
}

Storage.prototype.checksum = function (s){
	var i;
	var chk = 0x12345678;
	for (i = 0; i < s.length; i++) {
		chk += (s.charCodeAt(i) * (i + 1));
	}
	return chk+"";
}

Storage.prototype.createFileThumb = function(args, callback){
	var tmpFileName = __dirname+ '/tmp/'+UUID.v1()+'.jpg';
	var isVideo 	= args.isVideo;
	var path 		= args.path;

	Helper.extractVideoThumb(path, tmpFileName, '00:00:01', null, function(err, destPath){
		if (err)
			return callback(err);

		callback(null, destPath);
	});
}

Storage.prototype.storeFile = function(db, file, fileName, metadata, callback){
	var self = this;

	if (!file.originalFilename) {//if file not uploaded
		return callback(null, {});
	};

	if( !fs.existsSync(file.path) ){
		return callback('temp file missing.');
	}
	metadata 				= metadata?metadata:{};

	self.saveFile(db, file, fileName, metadata, function(err, result){
		if (err)
			return callback(err);

		//var isVideo = file.headers['content-type'].indexOf('video')==0;

		//if (!isVideo)
			return callback(null, result);

		self.createFileThumb({path: file.path}, function(err, thumbPath){
			if (err)
				return callback(err);

			metadata.isThumb = true;
			self.saveFile(db, {originalFilename: '', path: thumbPath, headers:{"content-type": 'image/jpg'} }, result.filename + '-thumb', metadata, function(err){
				if (err)
					return callback(err);

				fs.unlinkSync(thumbPath);

				self.updateMetaData({filename: fileName}, {thumb: fileName + '-thumb'}, function(err){
					if (err)
						return callback(err);

					callback(null, result);
				})
			});
		})
	})
}

Storage.prototype.updateMetaData = function(condition, metadata, callback){
	var self = this, data = {};
	_.each(metadata, function(value, key){
		data['metadata.'+key] = value;
	});

	self.db.collection('fs.files').update(condition, {$set: data }, function (err) {
		if (err)
			callback(err);

		callback(null, {success: true});
	});
}

Storage.prototype.saveFile = function(db, file, fileName, metadata, callback){
	var self = this;

	metadata.file_name 		= file.originalFilename;
	metadata.id 			= UUID.v1();
	var data;
	if (file.content) {
		data  = new Buffer(file.content.data || file.content);
	}else{
		data  = fs.readFileSync(file.path);
	}
	//console.log("data.length:".greenBG, file.content, data, data.length, (data.toString()).length)
	if (!data)
		return callback({error: "Unable to read file contents", code:"FILE-CONTENT-FAILED"});

	if (!fileName) {
		if (self._useContentHashAsName) {
			fileName = self.createHash( data.toString() );
		}else{
			fileName = file.originalFilename
		}
	}


	var gridStore = new self.GridStore(db, fileName, 'w', {metadata: metadata, content_type: file.headers['content-type'] });

	// Open the file
	gridStore.open(function(err, gridStore) {
		if (err)
			return callback(err);

		// Write some data to the file
		gridStore.write(data, function(err, gridStore) {
			if (err)
				return callback(err);

			// Close (Flushes the data to MongoDB)
			gridStore.close(function(err, result) {
				if (err)
					return callback(err);

				self.GridStore.read(db, result._id, function(err, fileData) {
					if (err)
						return callback(err);

					if(data.length != fileData.length)
						return callback({error: 'Error in saving file.', length: data.length +","+ fileData.length });

					result.filename = fileName;
					callback(null, result);
				});
			});
		});
	});
};

Storage.prototype.updateFile = function (req, validateFn, callbackFn, db) {
    var self = this;

    self.parseReq(req, function (err, fields, files) {
        if (err)
            return callbackFn(err);

        validateFn(fields, files, function (err, id, file) {
            if (err)
                return callbackFn(err);

            self._updateFile(db || self.db, id, file, callbackFn);
        })
    });
};

Storage.prototype._updateFile = function (db, id, file, callback) {
    var self = this;

    if (!file.originalFilename) {//if file not uploaded
        return callback(null, {});
    }

    if (!fs.existsSync(file.path)) {
        return callback(new Error('temp file missing.'));
    }

    db.collection('fs.files').findOne({filename: id}, function (err, item) {
        if (err)
            return callback(err);

        if (!item)
            return callback(new Error('File not found'));


        var metadata = item.metadata;
        metadata.file_name = file.originalFilename;

        var data = fs.readFileSync(file.path);
        var gridStore = new self.GridStore(db, id, 'w', {metadata: metadata, content_type: file.headers['content-type'] });

        gridStore.open(function (err, gridStore) {
            if (err)
                return callback(err);

            gridStore.write(data, function (err, gridStore) {
                if (err)
                    return callback(err);

                gridStore.close(function (err, result) {
                    if (err)
                        return callback(err);

                    self.GridStore.read(db, result._id, function (err, fileData) {
                        if (err)
                            return callback(err);

                        if (data.length != fileData.length)
                            return callback(new Error('Error in saving file.')); // TODO VRuden and is it finish???

                        db.collection('fs.files').update({filename: id}, {$set: {uploadDate: new Date()}}, function (err) { // TODO VRuden may be it must be up
                            if (err)
                                console.error('Timestamp of file:', id, 'was not updated');
                        });

                        callback(null, result);
                    });
                });
            });
        });
    });
};
Storage.prototype.readFile = function(filename, callback, db){
	var self = this, gridStore = new self.GridStore(db || self.db, filename, "r");
	gridStore.open(function( err, result ) {
		if (err)
			return callback(err);

		callback(null, result);
	});
};
Storage.prototype.read = function(filename, callback, db){
	var self = this;
	self.GridStore.read(db || self.db, filename, function( err, result ) {
		if (err)
			return callback(err);

		callback(null, result);
	});
};

Storage.prototype.getFile = function(filename, callback){
	var self = this;
	self.db.collection('fs.files').findOne({filename: filename}, function (err, file) {
        if (err)
            return callback(err);

        callback(null, file);
    });
};

/**
 * @param query {Array} Array of filename(id)
 */
Storage.prototype.getFileMetadata = function (query, callback) {
    var self = this;

    self.db.collection('fs.files').find({filename: {$in: query}}).toArray(function (err, items) {
        if (err)
            return callback(err);

        var result = {};
        _.each(items, function (item) {
            result[item.filename] = {
                timestamp: new Date(item.uploadDate).getTime(),
                size: item.length,
                type: item.contentType
            };
        });

        _.each(query, function(id) {
            if (!_.has(result, id)) {
                result[id] = null;
            }
        });

        callback(null, result);
    });
};

Storage.prototype.removeOne = function(condition, callback, db){
	var self = this;
	self.list(condition, { fields:{_id:1, filename:1}, limit:1}, function(err, records, total){
		if (err)
			return callback(err);

		if ( total > 1)
			return callback({error:'More than one record'});

		if ( total < 1)
			return callback({error:'No such record'});

		self.readFile(records[0].filename, function(err, file){
			if (err)
				return callback(err);

			file.unlink(function(err, result) {
				if (err)
					return callback(err);

				result.removedFile = records[0];
				callback(null, result);
			});
		});
	});
};

/**
*
*     condition: {Object} query condition:
*     {
*         'metadata.folder':'3'
*     }
*     options: {
*         fields: {Object} an object containing what fields to include or exclude from objects returned {id: 1}
*         sort: {Object} how to sort the query results: {name:1}
*         start: {Number} number rows of query result to be skipped
*         limit: {Number} maximum number of records to be returned
*     }
*
*/

Storage.prototype.list = function(condition, options, callbackFn, db){
	var self = this;
	options.query = condition;

	self._list(db || self.db, options, function( err, result, total ) {
		if (err)
			return callbackFn(err);

		callbackFn(null, result, total);
	});
};

Storage.prototype._list = function(db, rootCollection, options, callback) {
	var self = this;
	var args = Array.prototype.slice.call(arguments, 1);
	callback = args.pop();
	rootCollection = args.length ? args.shift() : null;
	options = args.length ? args.shift() : {};

	// Ensure we have correct values
	if(rootCollection != null && typeof rootCollection == 'object') {
		options = rootCollection;
		rootCollection = null;
	}

	// Establish read preference
	var readPreference = options.readPreference || 'primary';

	var start 		= options.start? parseInt(options.start) : 0;
	var limit		= options.limit? parseInt(options.limit) : 0;
	var fields		= ( typeof options.fields == 'object' ) ? options.fields : { };
	var query		= ( typeof options.query == 'object' ) ? options.query : { };


	// Fetch item
	var rootCollectionFinal = rootCollection != null ? rootCollection : self.GridStore.DEFAULT_ROOT_COLLECTION;
	var items = [];
	db.collection((rootCollectionFinal + ".files"), function(err, collection) {
		if(err)
			return callback(err);

		collection.find(query, fields, {readPreference:readPreference, limit:limit, skip:start}, function(err, cursor) {
		if(err)
			return callback(err);

		options.sort && cursor.sort(options.sort);

			cursor.toArray(function(err, records){
				if (err)
					return callback(err);

				cursor.count(function(err, total){
					if (err)
						return callback(err);

					callback(null, records, total);
				});
			});
		});
	});
};

module.exports = Storage;
