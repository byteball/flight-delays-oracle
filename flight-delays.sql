# log all responses for audit

CREATE TABLE fd_responses (
	device_address CHAR(33) NOT NULL,
	feed_name TEXT NOT NULL,
	creation_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	response TEXT NOT NULL,
	FOREIGN KEY (device_address) REFERENCES correspondent_devices(device_address)
);

CREATE INDEX byFdResponsesDeviceAddress ON fd_responses(device_address);

