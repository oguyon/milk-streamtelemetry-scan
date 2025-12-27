#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <math.h>
#include <sys/ioctl.h>

// ASCII Greyscale map
const char *GREYSCALE = " .:-=+*#%@";

typedef struct {
    char name[256];
    long total_frames;
    int *bins;
    int max_bin_count;
} Stream;

typedef struct {
    Stream *streams;
    int count;
    int capacity;
} StreamList;

typedef struct {
    int is_count_line; // 1 if this is "XX files", 0 if change line
    int count; // Used if is_count_line
    // Used if !is_count_line
    char keyname[80];
    double ts;
    char status[16];
    char value[256];
} ReportLine;

typedef struct {
    ReportLine *lines;
    int count;
    int capacity;
} Report;

typedef struct {
    char target_key[80];
    char target_stream[256]; // Empty if scanning all
    char last_value[256];
    int count_same_val;
    int has_last_value;
    Report report;
} KeyScanContext;

void init_report(Report *r) {
    r->count = 0;
    r->capacity = 10;
    r->lines = malloc(r->capacity * sizeof(ReportLine));
}

void add_report_line(Report *r, ReportLine line) {
    if (r->count == r->capacity) {
        r->capacity *= 2;
        r->lines = realloc(r->lines, r->capacity * sizeof(ReportLine));
    }
    r->lines[r->count++] = line;
}

void free_report(Report *r) {
    free(r->lines);
}

void init_stream_list(StreamList *list) {
    list->count = 0;
    list->capacity = 10;
    list->streams = malloc(list->capacity * sizeof(Stream));
}

Stream* get_or_create_stream(StreamList *list, const char *name, int num_bins) {
    for (int i = 0; i < list->count; i++) {
        if (strcmp(list->streams[i].name, name) == 0) {
            return &list->streams[i];
        }
    }
    if (list->count == list->capacity) {
        list->capacity *= 2;
        list->streams = realloc(list->streams, list->capacity * sizeof(Stream));
    }
    Stream *s = &list->streams[list->count++];
    strncpy(s->name, name, 255);
    s->name[255] = '\0';
    s->total_frames = 0;
    s->bins = calloc(num_bins, sizeof(int));
    s->max_bin_count = 0;
    return s;
}

void free_stream_list(StreamList *list) {
    for (int i = 0; i < list->count; i++) {
        free(list->streams[i].bins);
    }
    free(list->streams);
}

int is_directory(const char *path) {
    struct stat statbuf;
    if (stat(path, &statbuf) != 0) return 0;
    return S_ISDIR(statbuf.st_mode);
}

double parse_time_arg(const char *arg) {
    if (strncmp(arg, "UT", 2) == 0) {
        // Parse UTYYYYMMDDTHH:MM:SS
        struct tm tm_val;
        memset(&tm_val, 0, sizeof(struct tm));
        int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;

        int count = sscanf(arg, "UT%4d%2d%2dT%2d:%2d:%2d", &year, &month, &day, &hour, &minute, &second);
        if (count >= 4) {
            tm_val.tm_year = year - 1900;
            tm_val.tm_mon = month - 1;
            tm_val.tm_mday = day;
            tm_val.tm_hour = hour;
            tm_val.tm_min = minute;
            tm_val.tm_sec = second;
            return (double)timegm(&tm_val);
        } else {
             fprintf(stderr, "Warning: Failed to parse UT time format: %s. usage: UTYYYYMMDDTHH[:MM[:SS]]\n", arg);
             return 0.0;
        }
    } else {
        return atof(arg);
    }
}

void format_time_iso(double ts, char *buf, size_t size) {
    time_t t = (time_t)ts;
    struct tm tm_val;
    gmtime_r(&t, &tm_val);
    snprintf(buf, size, "UT%04d%02d%02dT%02d:%02d:%02d",
             tm_val.tm_year + 1900, tm_val.tm_mon + 1, tm_val.tm_mday,
             tm_val.tm_hour, tm_val.tm_min, tm_val.tm_sec);
}

// Function to process a single .txt file
void process_file(const char *filepath, const char *stream_name, double tstart, double tend, StreamList *streams, int num_bins) {
    FILE *fp = fopen(filepath, "r");
    if (!fp) return;

    char line[1024];
    Stream *s = NULL;

    // Optimization: Only lookup stream once we find valid frames or just once per file?
    // Once per file is safer.
    s = get_or_create_stream(streams, stream_name, num_bins);

    while (fgets(line, sizeof(line), fp)) {
        if (line[0] == '#') continue;

        // Parse columns. We need column 5 (0-indexed 4).
        // Format is space separated.
        char *ptr = line;
        int col = 0;
        double timestamp = 0.0;
        int found = 0;

        // Simple tokenizer
        char *token = strtok(line, " \t");
        while (token) {
            if (col == 4) { // 5th column
                timestamp = atof(token);
                found = 1;
                break;
            }
            token = strtok(NULL, " \t");
            col++;
        }

        if (found) {
            if (timestamp >= tstart && timestamp <= tend) {
                s->total_frames++;
                int bin = (int)((timestamp - tstart) / (tend - tstart) * num_bins);
                if (bin < 0) bin = 0;
                if (bin >= num_bins) bin = num_bins - 1;
                s->bins[bin]++;
                if (s->bins[bin] > s->max_bin_count) {
                    s->max_bin_count = s->bins[bin];
                }
            }
        }
    }
    fclose(fp);
}

void trim_fits_value(char *val) {
    // Remove leading spaces
    char *start = val;
    while (*start == ' ') start++;

    // Remove trailing spaces
    char *end = start + strlen(start) - 1;
    while (end > start && (*end == ' ' || *end == '\n' || *end == '\r')) end--;
    *(end + 1) = '\0';

    // Remove quotes if string
    if (*start == '\'' && *end == '\'') {
        start++;
        *end = '\0';
        // Check for trailing quote spaces?
        char *new_end = start + strlen(start) - 1;
        while (new_end > start && *new_end == ' ') new_end--;
        *(new_end+1) = '\0';
    }

    memmove(val, start, strlen(start) + 1);
}

int read_header_keyword(const char *filepath, const char *key, char *value_out) {
    FILE *fp = fopen(filepath, "r");
    if (!fp) return 0;

    char line[81]; // FITS header lines are 80 chars
    int found = 0;

    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, key, strlen(key)) == 0) {
            // Check if followed by space or =
            // FITS standard: KEYNAME = value / comment
            // KEYNAME is usually 8 chars, padded with spaces if shorter.
            // If key provided is shorter than 8 chars, we should check strict match?
            // Let's assume user provides exact key string match at start.

            char *eq_pos = strchr(line, '=');
            if (eq_pos) {
                // Value is after =
                char *slash_pos = strchr(eq_pos, '/');
                if (slash_pos) *slash_pos = '\0'; // Remove comment

                strncpy(value_out, eq_pos + 1, 255);
                value_out[255] = '\0';
                trim_fits_value(value_out);
                found = 1;
                break;
            }
        }
    }
    fclose(fp);
    return found;
}

// Global context for keyword scanning
KeyScanContext kscan_ctx;

void process_header_for_key(const char *header_path, double file_timestamp) {
    char current_val[256];
    if (read_header_keyword(header_path, kscan_ctx.target_key, current_val)) {
        if (!kscan_ctx.has_last_value) {
            ReportLine line;
            line.is_count_line = 0;
            strncpy(line.keyname, kscan_ctx.target_key, 79);
            line.ts = file_timestamp;
            strcpy(line.status, "INITIAL");
            strncpy(line.value, current_val, 255);
            add_report_line(&kscan_ctx.report, line);

            strncpy(kscan_ctx.last_value, current_val, 255);
            kscan_ctx.count_same_val = 1;
            kscan_ctx.has_last_value = 1;
        } else {
            if (strcmp(current_val, kscan_ctx.last_value) != 0) {
                // Add count line
                ReportLine count_line;
                count_line.is_count_line = 1;
                count_line.count = kscan_ctx.count_same_val;
                add_report_line(&kscan_ctx.report, count_line);

                // Add change line
                ReportLine change_line;
                change_line.is_count_line = 0;
                strncpy(change_line.keyname, kscan_ctx.target_key, 79);
                change_line.ts = file_timestamp;
                strcpy(change_line.status, "CHANGE");
                strncpy(change_line.value, current_val, 255);
                add_report_line(&kscan_ctx.report, change_line);

                strncpy(kscan_ctx.last_value, current_val, 255);
                kscan_ctx.count_same_val = 1;
            } else {
                kscan_ctx.count_same_val++;
            }
        }
    }
}

void scan_stream_dir(const char *path, const char *stream_name, double tstart, double tend, StreamList *streams, int num_bins) {
    struct dirent **namelist;
    int n = scandir(path, &namelist, NULL, alphasort);
    if (n < 0) return;

    for (int i = 0; i < n; i++) {
        struct dirent *dir = namelist[i];
        if (dir->d_name[0] == '.') {
            free(namelist[i]);
            continue;
        }

        // Check if it ends with .txt
        size_t len = strlen(dir->d_name);
        if (len > 4 && strcmp(dir->d_name + len - 4, ".txt") == 0) {
            char filepath[1024];
            snprintf(filepath, sizeof(filepath), "%s/%s", path, dir->d_name);

            // Extract time from filename to check range roughly?
            // Filename: sname_hh:mm:ss.sssssssss.txt
            // We only have HH:MM:SS... in filename, day is in directory.
            // But process_file reads actual timestamps.

            // If scanning keywords, we need chronologial order.
            // scandir alphasort gives chronological order for "sname_HH:MM:SS..." files within the same day.

            // For keyword scanning:
            if (kscan_ctx.target_key[0] != '\0') {
                int process_this = 1;
                if (kscan_ctx.target_stream[0] != '\0' && strcmp(stream_name, kscan_ctx.target_stream) != 0) {
                    process_this = 0;
                }

                if (process_this) {
                    // Check header file
                    char headerpath[1024];
                    // Replace .txt with .fits.header
                    // Assuming name is sname_....txt
                    // header is sname_....fits.header
                    strncpy(headerpath, filepath, sizeof(headerpath));
                    headerpath[strlen(filepath) - 4] = '\0'; // Remove .txt
                    strncat(headerpath, ".fits.header", sizeof(headerpath) - strlen(headerpath) - 1);

                    // We need a timestamp for the file.
                    // process_file gets individual frame times.
                    // We can peek at the first valid time in the txt file?
                    // Or parse the filename?
                    // Filename only has time of day. We need full time.
                    // But we are inside `scan_stream_dir` which is inside a day loop.
                    // We don't have the "day" easily accessible here except via path traversal or parsing path.
                    // Actually, process_file reads the absolute time from the file content.
                    // Let's modify process_file to return the first timestamp?
                    // Or just read the first line of the file manually here?

                    // Let's just peek the first line of the .txt file to get the time.
                    FILE *fp = fopen(filepath, "r");
                    if (fp) {
                        char line[1024];
                        double file_ts = 0.0;
                        while(fgets(line, sizeof(line), fp)) {
                            if (line[0] == '#') continue;
                            // col 5
                            char *token = strtok(line, " \t");
                            int col = 0;
                            while (token) {
                                if (col == 4) {
                                    file_ts = atof(token);
                                    break;
                                }
                                token = strtok(NULL, " \t");
                                col++;
                            }
                            if (file_ts > 0) break;
                        }
                        fclose(fp);

                        if (file_ts >= tstart && file_ts <= tend) {
                            process_header_for_key(headerpath, file_ts);
                        }
                    }
                }
            }

            process_file(filepath, stream_name, tstart, tend, streams, num_bins);
        }
        free(namelist[i]);
    }
    free(namelist);
}

int main(int argc, char *argv[]) {
    // Parse arguments
    // Simple manual parsing since we have positional args
    // milk-streamtelemetry-scan [-k KEY] <dir> <tstart> <tend>

    char *root_dir = NULL;
    char *tstart_str = NULL;
    char *tend_str = NULL;

    kscan_ctx.target_key[0] = '\0';
    kscan_ctx.target_stream[0] = '\0';
    kscan_ctx.has_last_value = 0;
    init_report(&kscan_ctx.report);

    int pos_arg_count = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-k") == 0) {
            if (i + 1 < argc) {
                char *karg = argv[++i];
                // Check if sname:keyname
                char *colon = strchr(karg, ':');
                if (colon) {
                    *colon = '\0';
                    strncpy(kscan_ctx.target_stream, karg, 255);
                    strncpy(kscan_ctx.target_key, colon + 1, 79);
                } else {
                    strncpy(kscan_ctx.target_key, karg, 79);
                }
            } else {
                fprintf(stderr, "Error: -k requires an argument\n");
                return 1;
            }
        } else {
            if (pos_arg_count == 0) root_dir = argv[i];
            else if (pos_arg_count == 1) tstart_str = argv[i];
            else if (pos_arg_count == 2) tend_str = argv[i];
            pos_arg_count++;
        }
    }

    if (pos_arg_count < 3) {
        fprintf(stderr, "Usage: %s [-k KEY] <dir> <tstart> <tend>\n", argv[0]);
        return 1;
    }

    double tstart = parse_time_arg(tstart_str);
    double tend = parse_time_arg(tend_str);

    if (tstart >= tend) {
        fprintf(stderr, "Error: tstart must be less than tend\n");
        return 1;
    }

    // Determine terminal width
    struct winsize w;
    int term_width = 80;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) != -1) {
        term_width = w.ws_col;
    }

    int name_width = 20;
    int timeline_width = term_width - name_width - 2; // -2 for spacing
    if (timeline_width < 10) timeline_width = 10;

    StreamList stream_list;
    init_stream_list(&stream_list);

    // Iterate through dates from tstart to tend
    time_t current_t = (time_t)tstart;
    time_t end_t = (time_t)tend;

    struct tm start_tm_struct;
    gmtime_r(&current_t, &start_tm_struct);

    start_tm_struct.tm_hour = 0;
    start_tm_struct.tm_min = 0;
    start_tm_struct.tm_sec = 0;
    time_t iter_t = timegm(&start_tm_struct);

    struct tm end_tm_struct;
    gmtime_r(&end_t, &end_tm_struct);
    end_tm_struct.tm_hour = 0;
    end_tm_struct.tm_min = 0;
    end_tm_struct.tm_sec = 0;
    time_t end_iter_t = timegm(&end_tm_struct);

    // Loop through days
    for (time_t t = iter_t; t <= end_iter_t + 10; t += 86400) {
        if (t > end_iter_t) break;

        struct tm tm_date;
        gmtime_r(&t, &tm_date);
        char date_str[32];
        snprintf(date_str, sizeof(date_str), "%04d%02d%02d",
                 tm_date.tm_year + 1900, tm_date.tm_mon + 1, tm_date.tm_mday);

        char date_path[1024];
        snprintf(date_path, sizeof(date_path), "%s/%s", root_dir, date_str);

        if (is_directory(date_path)) {
            // Scan subdirectories (streams)
            // Need sorted stream names too if we want deterministic order?
            // Ideally yes, but scan_stream_dir processes each stream fully.
            // If user did not specify stream, we process all streams.
            // For keyword scanning, mixing streams in output might be weird if single timeline.
            // But let's assume we do it per stream dir visit.

            struct dirent **namelist;
            int n = scandir(date_path, &namelist, NULL, alphasort);
            if (n >= 0) {
                for (int i = 0; i < n; i++) {
                    struct dirent *dir = namelist[i];
                    if (dir->d_name[0] == '.') {
                        free(namelist[i]);
                        continue;
                    }

                    char stream_path[2048];
                    snprintf(stream_path, sizeof(stream_path), "%s/%s", date_path, dir->d_name);

                    if (is_directory(stream_path)) {
                        scan_stream_dir(stream_path, dir->d_name, tstart, tend, &stream_list, timeline_width);
                    }
                    free(namelist[i]);
                }
                free(namelist);
            }
        }
    }

    // If we were scanning keys, we might have a final value pending
    if (kscan_ctx.has_last_value) {
        ReportLine count_line;
        count_line.is_count_line = 1;
        count_line.count = kscan_ctx.count_same_val;
        add_report_line(&kscan_ctx.report, count_line);

        ReportLine end_line;
        end_line.is_count_line = 0;
        strncpy(end_line.keyname, kscan_ctx.target_key, 79);
        end_line.ts = 0; // No specific timestamp for END marker line value? Or last processed?
                         // User said: "add line for ... end (last) value for the keyword"
                         // Usually we just show the final state. Timestamp isn't really applicable or is the end of scan.
                         // Let's use 0 or something else. User asked for "fields unix time, UT time...".
                         // For "END" line, maybe use end time of scan? Or just leave blank?
                         // Let's use 0.0 and handle printing to maybe show "-" or similar.
        end_line.ts = tend; // Use scan end time?
        strcpy(end_line.status, "END");
        strncpy(end_line.value, kscan_ctx.last_value, 255);
        add_report_line(&kscan_ctx.report, end_line);
    }

    // Print output
    printf("\nSummary:\n");

    char start_str[64];
    char end_str[64];
    format_time_iso(tstart, start_str, sizeof(start_str));
    format_time_iso(tend, end_str, sizeof(end_str));

    double dt_per_char = (tend - tstart) / timeline_width;
    double duration = tend - tstart;

    printf("Start: %s  End: %s  Duration: %.3f s  Bin: %.3f s\n",
           start_str, end_str, duration, dt_per_char);

    printf("\nTimeline:\n");

    // Print Header
    printf("%-*s ", name_width, "");

    int show_s = (dt_per_char < 2.0);
    int show_m = (dt_per_char < 120.0);
    int show_h = (dt_per_char < 7200.0);

    for (int i = 0; i < timeline_width; i++) {
        char marker = ' ';
        double t0 = tstart + i * dt_per_char;
        double t1 = tstart + (i + 1) * dt_per_char;

        time_t time0 = (time_t)t0;
        time_t time1 = (time_t)t1;

        if (time0 != time1) {
            struct tm tm0;
            struct tm tm1;
            gmtime_r(&time0, &tm0);
            gmtime_r(&time1, &tm1);

            if (tm0.tm_year != tm1.tm_year || tm0.tm_yday != tm1.tm_yday) {
                marker = 'D';
            } else if (tm0.tm_hour != tm1.tm_hour) {
                if (show_h) marker = 'H';
            } else if (tm0.tm_min != tm1.tm_min) {
                if (show_m) marker = 'M';
            } else if (tm0.tm_sec != tm1.tm_sec) {
                if (show_s) marker = 'S';
            }
        }
        putchar(marker);
    }
    printf("\n");

    for (int i = 0; i < stream_list.count; i++) {
        Stream *s = &stream_list.streams[i];
        if (s->total_frames == 0) continue;

        // [COUNT] STREAM_NAME
        char name_buf[256];
        snprintf(name_buf, sizeof(name_buf), "[%ld] %s", s->total_frames, s->name);
        printf("%-*s ", name_width, name_buf);

        for (int b = 0; b < timeline_width; b++) {
            int count = s->bins[b];
            char c = ' ';
            if (count > 0) {
                if (s->max_bin_count > 0) {
                    int idx = 1 + (int)((double)count * 8.999 / s->max_bin_count);
                    if (idx > 9) idx = 9;
                    if (idx < 1) idx = 1;
                    c = GREYSCALE[idx];
                } else {
                    c = '.';
                }
            }
            putchar(c);
        }
        printf("\n");
    }

    printf("\nLegend: ' ' = 0 frames. Non-space characters show relative density (normalized to peak frame rate per stream).\n");
    printf("Scale: ");
    for (int i = 0; i < 10; i++) {
        printf("%c", GREYSCALE[i]);
    }
    printf(" (Low -> High density)\n");

    // Print Keyword Report
    if (kscan_ctx.report.count > 0) {
        printf("\nKeyword Scan Report:\n");
        // "Keyname, UT time, unix time, status (INITIAL, CHANGE, END) and Keyword value, all aligned."
        // "Intermediate lines ... should start with 8 blanks chars and then print “XX files”"

        // Define column widths
        // Keyname: 20
        // UT time: 24
        // Unix time: 18
        // Status: 10
        // Value: rest

        for (int i = 0; i < kscan_ctx.report.count; i++) {
            ReportLine *l = &kscan_ctx.report.lines[i];
            if (l->is_count_line) {
                printf("        %d files\n", l->count);
            } else {
                char time_str[64];
                format_time_iso(l->ts, time_str, sizeof(time_str));

                // Keyname, UT time, unix time, status, Keyword value
                printf("%-20s %-24s %-18.6f %-10s %s\n",
                       l->keyname,
                       time_str,
                       l->ts,
                       l->status,
                       l->value);
            }
        }
    }

    free_report(&kscan_ctx.report);
    free_stream_list(&stream_list);
    return 0;
}
