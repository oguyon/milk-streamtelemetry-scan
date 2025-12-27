# milk-streamtelemetry-scan

Scan milk telemetry files on filesystem
Note: Telemetry format is specified in file TelemetryFormat.md in this repo.

## Compilation
Program is written in C, uses gcc and cmake for compilation
```bash
mkdir build
cd build
cmake ..
make
```

## Usage
```
milk-streamtelemetry-scan <dir> <tstart> <tend>
```

## Description
List telemetry files that contain frames between unix time stamps `<tstart>` and `<tend>` in directory `<dir>`. 
The program looks at timing files in the YYYYMMDD director(ies) matching the time range specified.

## Ouput
The program provides a summary of the telemetry data, as follow:
* For each stream for which data is being found, gives the number of frames within the time range. 
* Then plots a ASCII-format timeline with time from left to right of the terminal, with the stream name on the left, and use the remaining characters from left to right to encode time from tstart to tend. Use ASCII greyscale characters to show how many frames are acquired within the timebin corresponding to the character position from left to right. Prints a legend of ascii greyscale characters vs number of frames.

## Example use with telemetry sample included in this repo

```
./build/milk-streamtelemetry-scan telemetrysample 1762424400 1762424460
```
