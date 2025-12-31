# milk-streamtelemetry-scan

Scan milk telemetry files on filesystem
Note: Telemetry format is specified in file TelemetryFormat.md in this repo. An example telemetry directory is also provided (telemetrysample directory).

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

## Example 
In scexao6, /mnt/sdata05, for the engineering night of 11/06/2025, run: 
```
 ~/src/milk-streamtelemetry-scan/build/milk-streamtelemetry-scan -k OBJECT . UT20251106T05 UT20251106T08  
```
Here is the output: 
<img width="1492" height="831" alt="scan_output" src="https://github.com/user-attachments/assets/4ab47900-10cc-4822-b979-d144f63ad96c" />


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
