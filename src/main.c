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

void scan_stream_dir(const char *path, const char *stream_name, double tstart, double tend, StreamList *streams, int num_bins) {
    DIR *d = opendir(path);
    if (!d) return;

    struct dirent *dir;
    while ((dir = readdir(d)) != NULL) {
        if (dir->d_name[0] == '.') continue;

        // Check if it ends with .txt
        size_t len = strlen(dir->d_name);
        if (len > 4 && strcmp(dir->d_name + len - 4, ".txt") == 0) {
            char filepath[1024];
            snprintf(filepath, sizeof(filepath), "%s/%s", path, dir->d_name);
            process_file(filepath, stream_name, tstart, tend, streams, num_bins);
        }
    }
    closedir(d);
}

double parse_time_arg(const char *arg) {
    if (strncmp(arg, "UT", 2) == 0) {
        // Parse UTYYYYMMDDTHH:MM:SS (MM and SS optional)
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

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <dir> <tstart> <tend>\n", argv[0]);
        return 1;
    }

    char *root_dir = argv[1];
    double tstart = parse_time_arg(argv[2]);
    double tend = parse_time_arg(argv[3]);

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

    // We reserve some space for stream name (e.g. 20 chars) and borders.
    // Let's say 20 chars for name.
    int name_width = 20;
    int timeline_width = term_width - name_width - 2; // -2 for spacing
    if (timeline_width < 10) timeline_width = 10;

    StreamList stream_list;
    init_stream_list(&stream_list);

    // Iterate through dates from tstart to tend
    // We iterate day by day to find YYYYMMDD directories
    time_t current_t = (time_t)tstart;
    time_t end_t = (time_t)tend;

    // Normalize to start of day? No, just iterate.
    // But we need to handle boundaries correctly.
    // Simple approach: start at tstart, increment by 1 day until > tend.
    // Also include the day of tend.

    struct tm start_tm_struct;
    gmtime_r(&current_t, &start_tm_struct);

    // Create a time struct for iteration normalized to midnight
    start_tm_struct.tm_hour = 0;
    start_tm_struct.tm_min = 0;
    start_tm_struct.tm_sec = 0;
    time_t iter_t = timegm(&start_tm_struct);

    // Also get end day midnight
    struct tm end_tm_struct;
    gmtime_r(&end_t, &end_tm_struct);
    end_tm_struct.tm_hour = 0;
    end_tm_struct.tm_min = 0;
    end_tm_struct.tm_sec = 0;
    time_t end_iter_t = timegm(&end_tm_struct);

    // Loop through days
    for (time_t t = iter_t; t <= end_iter_t + 10; t += 86400) {
        // +10 just to be safe with leap seconds or whatever, loop condition is checked below
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
            DIR *d = opendir(date_path);
            if (d) {
                struct dirent *dir;
                while ((dir = readdir(d)) != NULL) {
                    if (dir->d_name[0] == '.') continue;

                    char stream_path[2048];
                    snprintf(stream_path, sizeof(stream_path), "%s/%s", date_path, dir->d_name);

                    if (is_directory(stream_path)) {
                        scan_stream_dir(stream_path, dir->d_name, tstart, tend, &stream_list, timeline_width);
                    }
                }
                closedir(d);
            }
        }
    }

    // Print output
    printf("Summary:\n");

    char start_str[64];
    char end_str[64];
    format_time_iso(tstart, start_str, sizeof(start_str));
    format_time_iso(tend, end_str, sizeof(end_str));

    printf("Start: %s\n", start_str);
    printf("End:   %s\n", end_str);

    double dt_per_char = (tend - tstart) / timeline_width;
    printf("Time per char: %.3f s\n", dt_per_char);

    for (int i = 0; i < stream_list.count; i++) {
        Stream *s = &stream_list.streams[i];
        if (s->total_frames > 0) {
            printf("Stream %s: %ld frames\n", s->name, s->total_frames);
        }
    }
    printf("\nTimeline:\n");

    // Print Header
    printf("%-*s ", name_width, "");

    // Decide marker density
    // We want to avoid printing finer markers if dt is large
    // If dt < 2.0s: Show S
    // If dt < 2.0m (120s): Show M
    // If dt < 2.0h (7200s): Show H

    int show_s = (dt_per_char < 2.0);
    int show_m = (dt_per_char < 120.0);
    int show_h = (dt_per_char < 7200.0);

    for (int i = 0; i < timeline_width; i++) {
        char marker = ' ';
        // Determine time range of this bin: [t0, t1)
        double t0 = tstart + i * dt_per_char;
        double t1 = tstart + (i + 1) * dt_per_char;

        // We check if a boundary is crossed within [t0, t1).
        // Since we are looking for integer second/minute/hour boundaries,
        // we can cast to time_t (which floors) and compare the start/end points.
        // Actually, converting to struct tm allows checking larger units easily.

        time_t time0 = (time_t)t0;
        time_t time1 = (time_t)t1; // Note: if t1 is exactly integer, it belongs to next bin usually.
        // However, standard "change" detection:

        // Let's compare the struct tm of t0 and t1.
        // If t1 is very close to integer, it might be the boundary itself.
        // Let's use a slightly offset t1 to represent the 'end' inclusive?
        // No, standard way: did the value change from t0 to t1?
        // t0 = 0.9, t1 = 1.9 -> Sec 0 to Sec 1. Change.

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

        printf("%-*s ", name_width, s->name);

        for (int b = 0; b < timeline_width; b++) {
            int count = s->bins[b];
            char c = ' ';
            if (count > 0) {
                if (s->max_bin_count > 0) {
                    // Map 1..max to 1..9
                    // index = 1 + (count * 8) / max
                    // But we want to use the full range.
                    // If count == max, index = 9
                    // If count == 1, index >= 1

                    // Let's use 10 chars: " .:-=+*#%@"
                    // 0 -> ' '
                    // 1..max maps to indices 1..9

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

    free_stream_list(&stream_list);
    return 0;
}
