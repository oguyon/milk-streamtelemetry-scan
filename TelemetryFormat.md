# Milk Telemetry Format

Telemetry refers to time sequences of 2D images saved as 3D FITS files, along with auxiliary ASCII files.

A sample of telemetry is included in this repo (directory telemetrysample), containing both timing data and header files. The FITS files are not included due to storage space.

## Naming conventions

Telemetry files for stream “sname” acquired at date YYYYMMDD, at UT time hh:mm:ss.sssssssss, is stored in consecutive sets of the following 3 files:
* FITS file: teldir/YYYYMMDD/sname/sname_hh:mm:ss.sssssssss.fits
* Timing data: teldir/YYYYMMDD/sname/sname_hh:mm:ss.sssssssss.txt
* Header file (optional): teldir/YYYYMMDD/sname/sname_hh:mm:ss.sssssssss.fits.header
where teldir is the telemetry directory.

Note: The timestamp in the file name indicates the start time for the whole file, so it matches the time of the first frame in the file. The next set of files should have a timestamp that matches the frame following the last frame of the previous set.
Frame rate should be constant over long periods of time, within and between consecutive files.




## FITS file

The pixel values (2D images) are saved as 3D FITS files. Datatype can be any of the floating points or integer types supported by the FITS standard. The using telemetry for computations, all types are loaded as single precision floating point unless otherwise specified.

The first 2 axes encode image spatial dimensions, and the 3rd axis is the frame index, encoding time. For example, the a FITS file containing 1000 frames, each an image of size 100x140 pixels, would have size 100x140x1000 (NAXIS1=100; NAXIS2=140; NAXIS3=1000)
Files can be compressed using fpack compression utility for FITS files, in which case the extension is .fits.fz


## Timing data

Each 3D FITS file has a matching timing file (.txt file extension).

The header part (starting with character #) provides a short description of the file. After the header, there is one line per frame. The number of lines equals the number of frames in the FITS file, and each line has 7 fields:
* col1 : datacube frame index
* col2 : Main index
* col3 : Time since cube origin (logging)
* col4 : Absolute time (logging)
* col5 : Absolute time (acquisition)
* col6 : stream cnt0 index
* col7 : stream cnt1 index

The absolute time (col5) is the timestamp to be used for synchronization between frames. This time indicates the end of the frame exposure.

Note: The timing data does not include the exposure time. It is safe to assume that the end of an exposure is the start of the next exposure, so the exposure time is the time offset between consecutive exposure start times.


## Header file (optional)

FITS headers are very useful to search for data, but reading header files from a large number of FITS files can be time-consuming, so the telemetry sometimes also includes separate FITS header files saved as ASCII files. The file extension is <basename>.fits.header
 and, optionally, a header (.fits.header)
The header file is a copy of the FITS header, written as a separate file to allow for fast searching according to keywords.

