# milk-streamtelemetry-scan

Scan milk telemetry files on filesystem

Usage:
milk-streamtelemetry-scan <dir> <tstart> <tend>


List telemetry files that contain frames between unix time stamps <tstart> and <tend> in directory <dir>. 

The program looks at timing files in the YYYYMMDD director(ies) matching the time range specified.

The program provides a summary of the telemetry data, as follow:
* For each stream for which data is being found, gives the number of frames within the time range. 
* Then plots a ASCII-format timeline with time from left to right of the terminal, with the stream name on the left 10 characters, and use the remaining characters from left to right to encode time from tstart to tend. Use ASCII greyscale characters to show how many frames are acquired within the timebin corresponding to the character position from left to right. Prints a legend of ascii greyscale characters vs number of frames.
