'use strict';

function parseHost(host) {
	var hostParts = host.split(':');

	return {
		host: hostParts[0],
		port: parseInt(hostParts[1])
	};
}

module.exports = function parseHostList(hosts) {
	if (hosts && hosts.trim().length > 0) {
		return hosts.split(',').map(parseHost);
	}

	return [];
};
