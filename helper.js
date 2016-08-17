var exec 	= require('child_process').exec;
var fs 		= require('fs');


function extractVideoThumb (path, destPath, time, size, callback) {
	if (time == null)
		time = '00:00:01';

	if (size == null)
		size = '200x125';

	var cmd = 'ffmpeg -ss ' + time + ' -r 1 -i "' + path + '" -y -s ' + size.replace('x', '*') + ' -f image2 "' + destPath+'"';
	console.log('cmd', cmd)

	return exec(cmd, function() {
		if (callback){
			if (!fs.existsSync(destPath))
				return callback({error: 'Could not create video thumbnail.'});

			return callback(null, destPath, cmd, arguments);
		}

	});
}

module.exports = {
	extractVideoThumb: extractVideoThumb
}
