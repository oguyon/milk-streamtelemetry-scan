#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <math.h>
#include <regex.h>
#include <sys/ioctl.h>

// Unicode Block Elements
const char *BLOCKS[] = {" ", "\u2581", "\u2582", "\u2583", "\u2584", "\u2585", "\u2586", "\u2587", "\u2588"};
// ANSI Colors (Blue -> Cyan -> Green -> Yellow -> Red)
const char *COLORS[] = {
    "\033[0m",        // 0: Reset (Space)
    "\033[38;5;21m",  // 1: Blue
    "\033[38;5;27m",  // 2: Blue-ish
    "\033[38;5;39m",  // 3: Cyan
    "\033[38;5;46m",  // 4: Green
    "\033[38;5;118m", // 5: Light Green
    "\033[38;5;154m", // 6: Yellow-Green
    "\033[38;5;220m", // 7: Yellow
    "\033[38;5;196m"  // 8: Red
};
#define RESET_COLOR "\033[0m"
#define BOLD_COLOR "\033[1m"

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
    char stream_name[256];
    char keyname[80];
    double ts;
    char status[16];
    char value[256];
    char filename[256];
} ReportLine;

typedef struct {
    ReportLine *lines;
    int count;
    int capacity;
} Report;

typedef struct {
    char stream_name[256];
    char key[80];
    char last_value[256];
    int count_same_val;
    int has_last_value;
} TrackedKey;

typedef struct {
    char target_key_pattern[256];
    regex_t key_regex;
    char target_stream[256]; // Empty if scanning all

    TrackedKey *tracked_keys;
    int tracked_count;
    int tracked_capacity;

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

// Global context for keyword scanning
KeyScanContext kscan_ctx;

TrackedKey* get_tracked_key(const char *stream, const char *key) {
    for (int i = 0; i < kscan_ctx.tracked_count; i++) {
        if (strcmp(kscan_ctx.tracked_keys[i].stream_name, stream) == 0 &&
            strcmp(kscan_ctx.tracked_keys[i].key, key) == 0) {
            return &kscan_ctx.tracked_keys[i];
        }
    }
    if (kscan_ctx.tracked_count == kscan_ctx.tracked_capacity) {
        kscan_ctx.tracked_capacity = (kscan_ctx.tracked_capacity == 0) ? 10 : kscan_ctx.tracked_capacity * 2;
        kscan_ctx.tracked_keys = realloc(kscan_ctx.tracked_keys, kscan_ctx.tracked_capacity * sizeof(TrackedKey));
    }
    TrackedKey *tk = &kscan_ctx.tracked_keys[kscan_ctx.tracked_count++];
    strncpy(tk->stream_name, stream, 255);
    strncpy(tk->key, key, 79);
    tk->key[79] = '\0';
    tk->last_value[0] = '\0';
    tk->count_same_val = 0;
    tk->has_last_value = 0;
    return tk;
}

void process_header_for_key(const char *header_path, const char *stream_name, double file_timestamp) {
    FILE *fp = fopen(header_path, "r");
    if (!fp) return;

    char line[82]; // FITS header lines are 80 chars

    while (fgets(line, sizeof(line), fp)) {
        // Parse key: chars before '=' or first 8 chars
        // FITS: "KEYNAME = value"
        char key[81];
        char value[256];
        int has_eq = 0;
        char *eq_pos = strchr(line, '=');

        if (eq_pos) {
            size_t key_len = eq_pos - line;
            if (key_len > 80) key_len = 80;
            strncpy(key, line, key_len);
            key[key_len] = '\0';

            // Trim key
            char *end = key + strlen(key) - 1;
            while (end >= key && (*end == ' ' || *end == '\t')) *end-- = '\0';

            // Regex check
            if (regexec(&kscan_ctx.key_regex, key, 0, NULL, 0) == 0) {
                // Extract value
                char *slash_pos = strchr(eq_pos, '/');
                if (slash_pos) *slash_pos = '\0'; // Remove comment

                strncpy(value, eq_pos + 1, 255);
                value[255] = '\0';
                trim_fits_value(value);

                // Track state
                TrackedKey *tk = get_tracked_key(stream_name, key);

                // Extract basename
                const char *base = strrchr(header_path, '/');
                if (base) base++; else base = header_path;

                if (!tk->has_last_value) {
                    ReportLine rl;
                    rl.is_count_line = 0;
                    strncpy(rl.stream_name, stream_name, 255);
                    strncpy(rl.keyname, key, 79);
                    rl.ts = file_timestamp;
                    strcpy(rl.status, "INITIAL");
                    strncpy(rl.value, value, 255);
                    strncpy(rl.filename, base, 255);
                    add_report_line(&kscan_ctx.report, rl);

                    strncpy(tk->last_value, value, 255);
                    tk->count_same_val = 1;
                    tk->has_last_value = 1;
                } else {
                    if (strcmp(value, tk->last_value) != 0) {
                        // Add count line
                        ReportLine count_line;
                        count_line.is_count_line = 1;
                        count_line.count = tk->count_same_val;
                        // Count line needs keyname to associate?
                        // The print logic assumes sequential.
                        // But with multiple keys/streams, sequential might be mixed.
                        // We store keyname in count line too just in case.
                        strncpy(count_line.keyname, key, 79);
                        strncpy(count_line.stream_name, stream_name, 255);
                        add_report_line(&kscan_ctx.report, count_line);

                        // Add change line
                        ReportLine change_line;
                        change_line.is_count_line = 0;
                        strncpy(change_line.stream_name, stream_name, 255);
                        strncpy(change_line.keyname, key, 79);
                        change_line.ts = file_timestamp;
                        strcpy(change_line.status, "CHANGE");
                        strncpy(change_line.value, value, 255);
                        strncpy(change_line.filename, base, 255);
                        add_report_line(&kscan_ctx.report, change_line);

                        strncpy(tk->last_value, value, 255);
                        tk->count_same_val = 1;
                    } else {
                        tk->count_same_val++;
                    }
                }
            }
        }
    }
    fclose(fp);
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

            // For keyword scanning:
            if (kscan_ctx.target_key_pattern[0] != '\0') {
                int process_this = 1;
                if (kscan_ctx.target_stream[0] != '\0' && strcmp(stream_name, kscan_ctx.target_stream) != 0) {
                    process_this = 0;
                }

                if (process_this) {
                    // Check header file
                    char headerpath[1024];
                    strncpy(headerpath, filepath, sizeof(headerpath));
                    headerpath[strlen(filepath) - 4] = '\0'; // Remove .txt
                    strncat(headerpath, ".fits.header", sizeof(headerpath) - strlen(headerpath) - 1);

                    // Peek time
                    FILE *fp = fopen(filepath, "r");
                    if (fp) {
                        char line[1024];
                        double file_ts = 0.0;
                        while(fgets(line, sizeof(line), fp)) {
                            if (line[0] == '#') continue;
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
                            process_header_for_key(headerpath, stream_name, file_ts);
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
    char *root_dir = NULL;
    char *tstart_str = NULL;
    char *tend_str = NULL;

    kscan_ctx.target_key_pattern[0] = '\0';
    kscan_ctx.target_stream[0] = '\0';
    kscan_ctx.tracked_keys = NULL;
    kscan_ctx.tracked_count = 0;
    kscan_ctx.tracked_capacity = 0;
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
                    strncpy(kscan_ctx.target_key_pattern, colon + 1, 255);
                } else {
                    strncpy(kscan_ctx.target_key_pattern, karg, 255);
                }

                if (regcomp(&kscan_ctx.key_regex, kscan_ctx.target_key_pattern, REG_EXTENDED | REG_NOSUB) != 0) {
                    fprintf(stderr, "Error: Invalid regex: %s\n", kscan_ctx.target_key_pattern);
                    return 1;
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
            // Scan subdirectories (streams) - Sorted to ensure deterministic order (apapane, then ocam2d)
            struct dirent **streamlist;
            int n_stream = scandir(date_path, &streamlist, NULL, alphasort);
            if (n_stream >= 0) {
                for (int i = 0; i < n_stream; i++) {
                    struct dirent *dir = streamlist[i];
                    if (dir->d_name[0] == '.') {
                        free(streamlist[i]);
                        continue;
                    }

                    char stream_path[2048];
                    snprintf(stream_path, sizeof(stream_path), "%s/%s", date_path, dir->d_name);

                    if (is_directory(stream_path)) {
                        scan_stream_dir(stream_path, dir->d_name, tstart, tend, &stream_list, timeline_width);
                    }
                    free(streamlist[i]);
                }
                free(streamlist);
            }
        }
    }

    // If we were scanning keys, we might have final values pending for multiple keys
    for (int i = 0; i < kscan_ctx.tracked_count; i++) {
        TrackedKey *tk = &kscan_ctx.tracked_keys[i];
        if (tk->has_last_value) {
            ReportLine count_line;
            count_line.is_count_line = 1;
            count_line.count = tk->count_same_val;
            strncpy(count_line.keyname, tk->key, 79);
            strncpy(count_line.stream_name, tk->stream_name, 255);
            add_report_line(&kscan_ctx.report, count_line);

            ReportLine end_line;
            end_line.is_count_line = 0;
            strncpy(end_line.stream_name, tk->stream_name, 255);
            strncpy(end_line.keyname, tk->key, 79);
            end_line.ts = tend; // Use scan end time
            strcpy(end_line.status, "END");
            strncpy(end_line.value, tk->last_value, 255);
            end_line.filename[0] = '\0'; // No file for end status
            add_report_line(&kscan_ctx.report, end_line);
        }
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

    // Calculate max label widths for alignment
    int max_name_len = 10;
    int max_count_len = 5;
    for (int i = 0; i < stream_list.count; i++) {
        Stream *s = &stream_list.streams[i];
        if (s->total_frames == 0) continue;
        int len = strlen(s->name);
        if (len > max_name_len) max_name_len = len;

        char count_buf[32];
        int clen = snprintf(count_buf, sizeof(count_buf), "%ld", s->total_frames);
        if (clen > max_count_len) max_count_len = clen;
    }

    // Total prefix width: Name + space + [ + Count + ] + space + FPS + space
    // StreamName [Count] 1234.5 Hz
    // Actual requested order: Stream name, number of frames, framerate, then framerate characters.
    // "stream name, then number of frames, then framerate, then framerate characters"
    // Let's format: "NAME   12345   123.4 Hz "

    int prefix_width = max_name_len + 3 + max_count_len + 3 + 10 + 1;
    // Name(max) + "   " + Count(max) + "   " + " 123.4 Hz "

    // Adjust timeline width: ensure we don't wrap. prefix_width + timeline_width <= term_width
    // We print prefix_width chars then a space, so prefix_width + 1 + timeline_width <= term_width
    timeline_width = term_width - prefix_width - 1;
    if (timeline_width < 10) timeline_width = 10;

    // Print Header
    printf("%-*s ", prefix_width, "");

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

        // Calculate max FPS
        double max_fps = 0.0;
        if (dt_per_char > 0.0) {
            max_fps = (double)s->max_bin_count / dt_per_char;
        }

        // Print prefix: Name(Bold) Count FPS
        printf(BOLD_COLOR "%-*s" RESET_COLOR "   %*ld   %6.1f Hz ",
               max_name_len, s->name,
               max_count_len, s->total_frames,
               max_fps);

        for (int b = 0; b < timeline_width; b++) {
            int count = s->bins[b];
            int idx = 0;
            if (count > 0) {
                if (s->max_bin_count > 0) {
                    idx = 1 + (int)((double)count * 7.999 / s->max_bin_count);
                    if (idx > 8) idx = 8;
                    if (idx < 1) idx = 1;
                } else {
                    idx = 1;
                }
            }
            // Print with color
            if (idx > 0) printf("%s", COLORS[idx]);
            printf("%s", BLOCKS[idx]);
            if (idx > 0) printf(RESET_COLOR);
        }
        printf("\n");

        // Render Keyword Timeline Row(s) if scanning
        if (kscan_ctx.target_key_pattern[0] != '\0') {
            // Find all unique keys for this stream in the report
            // Only consider unique keys that have some entries for this stream

            for (int k = 0; k < kscan_ctx.tracked_count; k++) {
                TrackedKey *tk = &kscan_ctx.tracked_keys[k];
                if (strcmp(tk->stream_name, s->name) != 0) continue;

                // Process timeline for this specific key on this stream
                char *key_line = malloc(timeline_width + 1);
                memset(key_line, ' ', timeline_width);
                key_line[timeline_width] = '\0';

                int has_key_entries = 0;

                // First pass: Mark changes with '|'
                for (int r = 0; r < kscan_ctx.report.count; r++) {
                    ReportLine *l = &kscan_ctx.report.lines[r];
                    if (l->is_count_line) continue;

                    if (strcmp(l->stream_name, s->name) != 0) continue;
                    if (strcmp(l->keyname, tk->key) != 0) continue;

                    // Map time to bin
                    if (l->ts >= tstart && l->ts <= tend) {
                        int bin = (int)((l->ts - tstart) / (tend - tstart) * timeline_width);
                        if (bin >= 0 && bin < timeline_width) {
                            key_line[bin] = '|';
                            has_key_entries = 1;
                        }
                    }
                }

                // Second pass: Fill values
                if (has_key_entries) {
                    for (int b = 0; b < timeline_width; b++) {
                        if (key_line[b] == '|') continue;

                        double bin_time = tstart + (b + 0.5) * dt_per_char;

                        const char *val = NULL;
                        for (int r = 0; r < kscan_ctx.report.count; r++) {
                            ReportLine *l = &kscan_ctx.report.lines[r];
                            if (l->is_count_line) continue;
                            if (strcmp(l->stream_name, s->name) != 0) continue;
                            if (strcmp(l->keyname, tk->key) != 0) continue;

                            if (l->ts <= bin_time) {
                                val = l->value;
                            } else {
                                break;
                            }
                        }

                        if (val) {
                            int prev_pipe = -1;
                            for (int p = b - 1; p >= 0; p--) {
                                if (key_line[p] == '|') {
                                    prev_pipe = p;
                                    break;
                                }
                            }

                            int dist = b - (prev_pipe == -1 ? -1 : prev_pipe);
                            int char_idx = dist - 1;
                            if (char_idx >= 0 && char_idx < (int)strlen(val)) {
                                key_line[b] = val[char_idx];
                            }
                        }
                    }

                    // Print the line
                    // Print keyname in the prefix area, right aligned
                    printf("%*s ", prefix_width, tk->key);
                    printf("%s\n", key_line);
                }
                free(key_line);
            }
        }
    }

    printf("\nLegend: ' ' = 0 frames. Blocks show relative density (normalized to peak frame rate per stream).\n");
    printf("Scale: ");
    for (int i = 0; i < 9; i++) {
        if (i > 0) printf("%s", COLORS[i]);
        printf("%s", BLOCKS[i]);
        if (i > 0) printf(RESET_COLOR);
    }
    printf(" (Low -> High density)\n");

    // Print Keyword Report
    if (kscan_ctx.report.count > 0) {
        printf("\nKeyword Scan Report:\n");

        for (int i = 0; i < kscan_ctx.report.count; i++) {
            ReportLine *l = &kscan_ctx.report.lines[i];
            if (l->is_count_line) {
                printf("        %d files\n", l->count);
            } else {
                char time_str[64];
                format_time_iso(l->ts, time_str, sizeof(time_str));

                // Keyname, UT time, unix time, status, Keyword value, Filename
                printf("%-20s %-24s %-18.6f %-10s %-20s %s\n",
                       l->keyname,
                       time_str,
                       l->ts,
                       l->status,
                       l->value,
                       l->filename);
            }
        }
    }

    free_report(&kscan_ctx.report);
    if (kscan_ctx.tracked_keys) free(kscan_ctx.tracked_keys);
    if (kscan_ctx.target_key_pattern[0] != '\0') regfree(&kscan_ctx.key_regex);
    free_stream_list(&stream_list);
    return 0;
}
