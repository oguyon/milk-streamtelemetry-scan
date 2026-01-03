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
#include <errno.h>

// Unicode Block Elements
const char *BLOCKS[] = {" ", "\u2588", "\u2588", "\u2588", "\u2588", "\u2588", "\u2588", "\u2588", "\u2588"};
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
#define BG_HIGHLIGHT_H "\033[41m" // Red background
#define BG_HIGHLIGHT_M "\033[44m" // Blue background
#define BG_SCALE "\033[48;5;237m" // Dark Grey background
#define BG_BLACK "\033[40m"       // Black background

// Cache constants
#define CACHE_DIR "cache"
#define CACHE_EXT ".cache"

typedef struct {
    int is_constant;
    long count;
    double start;
    double end;
    double *timestamps; // NULL if is_constant, otherwise array of size count
} FileSummary;

typedef struct {
    char *path;
    double timestamp;
} FileEntry;

typedef struct {
    char name[256];
    long total_frames;
    int *bins;
    int max_bin_count;
    FileEntry *files;
    int file_count;
    int file_capacity;
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

int compare_report_lines(const void *a, const void *b) {
    const ReportLine *ra = (const ReportLine *)a;
    const ReportLine *rb = (const ReportLine *)b;
    if (ra->ts < rb->ts) return -1;
    if (ra->ts > rb->ts) return 1;
    return rb->is_count_line - ra->is_count_line;
}

void init_stream_list(StreamList *list) {
    list->count = 0;
    list->capacity = 10;
    list->streams = malloc(list->capacity * sizeof(Stream));
}

Stream* get_or_create_stream(StreamList *list, const char *name) {
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
    s->bins = NULL;
    s->max_bin_count = 0;
    s->files = NULL;
    s->file_count = 0;
    s->file_capacity = 0;
    return s;
}

void add_file_to_stream(Stream *s, const char *path, double timestamp) {
    if (s->file_count == s->file_capacity) {
        s->file_capacity = (s->file_capacity == 0) ? 10 : s->file_capacity * 2;
        s->files = realloc(s->files, s->file_capacity * sizeof(FileEntry));
    }
    s->files[s->file_count].path = strdup(path);
    s->files[s->file_count].timestamp = timestamp;
    s->file_count++;
}

void free_stream_list(StreamList *list) {
    for (int i = 0; i < list->count; i++) {
        Stream *s = &list->streams[i];
        if (s->bins) free(s->bins);
        for (int j = 0; j < s->file_count; j++) {
            free(s->files[j].path);
        }
        if (s->files) free(s->files);
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

void trim_fits_value(char *val) {
    char *start = val;
    while (*start == ' ') start++;
    char *end = start + strlen(start) - 1;
    while (end > start && (*end == ' ' || *end == '\n' || *end == '\r')) end--;
    *(end + 1) = '\0';
    if (*start == '\'' && *end == '\'') {
        start++;
        *end = '\0';
        // Remove leading spaces inside quotes
        while (*start == ' ') start++;
        // Remove trailing spaces inside quotes
        char *new_end = start + strlen(start) - 1;
        while (new_end > start && *new_end == ' ') new_end--;
        *(new_end+1) = '\0';
    }
    memmove(val, start, strlen(start) + 1);
}

int read_header_keyword(const char *filepath, const char *key, char *value_out) {
    FILE *fp = fopen(filepath, "r");
    if (!fp) return 0;

    char line[82];
    int found = 0;
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, key, strlen(key)) == 0) {
            char *eq_pos = strchr(line, '=');
            if (eq_pos) {
                char *slash_pos = strchr(eq_pos, '/');
                if (slash_pos) *slash_pos = '\0';
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

void ensure_cache_dir_exists(const char *parent_dir) {
    char cache_path[4096];
    snprintf(cache_path, sizeof(cache_path), "%s/%s", parent_dir, CACHE_DIR);
    struct stat st = {0};
    if (stat(cache_path, &st) == -1) {
        if (mkdir(cache_path, 0755) != 0) {
            if (errno != EEXIST) {
                 fprintf(stderr, "Warning: Failed to create cache directory %s: %s\n", cache_path, strerror(errno));
            }
        }
    }
}

void ensure_path_exists(const char *filepath) {
    char temp[4096];
    strncpy(temp, filepath, sizeof(temp));
    temp[sizeof(temp) - 1] = '\0';

    // Remove filename to get dir path
    char *last_slash = strrchr(temp, '/');
    if (last_slash) {
        *last_slash = '\0';
        // Recursively create directories
        for (char *p = temp + 1; *p; p++) {
            if (*p == '/') {
                *p = '\0';
                if (mkdir(temp, 0755) != 0) {
                    if (errno != EEXIST) {
                        // Ignore error, maybe assume intermediate dir exists or permission issue will be caught later
                    }
                }
                *p = '/';
            }
        }
        if (mkdir(temp, 0755) != 0) {
            if (errno != EEXIST) {
                 fprintf(stderr, "Warning: Failed to create directory %s: %s\n", temp, strerror(errno));
            }
        }
    }
}

int read_cache(const char *cache_path, FileSummary *summary) {
    FILE *fp = fopen(cache_path, "r");
    if (!fp) return 0;

    char type[32];
    if (fscanf(fp, "%31s", type) != 1) { fclose(fp); return 0; }

    if (strcmp(type, "CONSTANT") == 0) {
        summary->is_constant = 1;
        if (fscanf(fp, "%ld %lf %lf", &summary->count, &summary->start, &summary->end) != 3) {
            fclose(fp);
            return 0;
        }
        summary->timestamps = NULL;
    } else if (strcmp(type, "RAW") == 0) {
        summary->is_constant = 0;
        if (fscanf(fp, "%ld", &summary->count) != 1) {
             fclose(fp); return 0;
        }
        summary->timestamps = malloc(summary->count * sizeof(double));
        for (long i = 0; i < summary->count; i++) {
            if (fscanf(fp, "%lf", &summary->timestamps[i]) != 1) {
                free(summary->timestamps);
                fclose(fp);
                return 0;
            }
        }
        // If count > 0, set start/end
        if (summary->count > 0) {
            summary->start = summary->timestamps[0];
            summary->end = summary->timestamps[summary->count - 1];
        } else {
            summary->start = 0;
            summary->end = 0;
        }
    } else {
        fclose(fp);
        return 0;
    }

    fclose(fp);
    return 1;
}

void write_cache(const char *cache_path, const FileSummary *summary) {
    FILE *fp = fopen(cache_path, "w");
    if (!fp) return;

    if (summary->is_constant) {
        fprintf(fp, "CONSTANT %ld %.9f %.9f\n", summary->count, summary->start, summary->end);
    } else {
        fprintf(fp, "RAW %ld\n", summary->count);
        for (long i = 0; i < summary->count; i++) {
            fprintf(fp, "%.9f\n", summary->timestamps[i]);
        }
    }
    fclose(fp);
}

// Global context for keyword scanning
KeyScanContext kscan_ctx;
int g_cache_export = 0;
long g_cache_searched = 0;
long g_cache_found = 0;
long g_cache_created = 0;

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

    char line[82];
    while (fgets(line, sizeof(line), fp)) {
        char key[81];
        char value[256];
        char *eq_pos = strchr(line, '=');

        if (eq_pos) {
            size_t key_len = eq_pos - line;
            if (key_len > 80) key_len = 80;
            strncpy(key, line, key_len);
            key[key_len] = '\0';

            char *end = key + strlen(key) - 1;
            while (end >= key && (*end == ' ' || *end == '\t')) *end-- = '\0';

            if (regexec(&kscan_ctx.key_regex, key, 0, NULL, 0) == 0) {
                char *slash_pos = strchr(eq_pos, '/');
                if (slash_pos) *slash_pos = '\0';
                strncpy(value, eq_pos + 1, 255);
                value[255] = '\0';
                trim_fits_value(value);

                TrackedKey *tk = get_tracked_key(stream_name, key);
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
                        ReportLine count_line;
                        count_line.is_count_line = 1;
                        count_line.count = tk->count_same_val;
                        count_line.ts = file_timestamp;
                        strncpy(count_line.keyname, key, 79);
                        strncpy(count_line.stream_name, stream_name, 255);
                        add_report_line(&kscan_ctx.report, count_line);

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

double parse_filename_time(const char *filename, const char *date_str) {
    // filename format: sname_HH:MM:SS.sssssssss.txt
    // date_str: YYYYMMDD
    // find the time part. It should be 18 chars before .txt
    size_t len = strlen(filename);
    if (len < 22) return 0.0; // Minimal check

    // Pointer to start of time string
    // Assuming format is always ending in ...HH:MM:SS.sssssssss.txt
    const char *time_part = filename + len - 4 - 18;

    // Reconstruct full ISO string: YYYYMMDDTHH:MM:SS.sssssssss
    // But we need to use timegm.

    struct tm tm_val;
    memset(&tm_val, 0, sizeof(struct tm));

    int year, month, day;
    if (sscanf(date_str, "%4d%2d%2d", &year, &month, &day) != 3) return 0.0;

    int hour, minute, second;
    double frac_sec = 0.0;

    if (sscanf(time_part, "%2d:%2d:%2d", &hour, &minute, &second) != 3) {
         return 0.0;
    }

    // Parse fractional seconds
    const char *frac_str = time_part + 8; // .sssssssss
    frac_sec = atof(frac_str);

    tm_val.tm_year = year - 1900;
    tm_val.tm_mon = month - 1;
    tm_val.tm_mday = day;
    tm_val.tm_hour = hour;
    tm_val.tm_min = minute;
    tm_val.tm_sec = second;

    time_t t = timegm(&tm_val);
    return (double)t + frac_sec;
}

void get_file_data(const char *filepath, FileSummary *summary) {
    g_cache_searched++;
    // Construct both potential cache paths
    char local_cache_path[8192];
    snprintf(local_cache_path, sizeof(local_cache_path), "%s/%s%s", CACHE_DIR, filepath, CACHE_EXT);

    char export_cache_path[8192];
    char *dir_sep = strrchr(filepath, '/');
    if (dir_sep) {
        char dir_path[4096];
        size_t dir_len = dir_sep - filepath;
        if (dir_len >= sizeof(dir_path)) dir_len = sizeof(dir_path) - 1;
        strncpy(dir_path, filepath, dir_len);
        dir_path[dir_len] = '\0';
        snprintf(export_cache_path, sizeof(export_cache_path), "%s/%s/%s%s", dir_path, CACHE_DIR, dir_sep + 1, CACHE_EXT);
    } else {
         snprintf(export_cache_path, sizeof(export_cache_path), "%s/%s%s", CACHE_DIR, filepath, CACHE_EXT);
    }

    // Try reading (Priority: Local, then Export)
    // Actually, user said: "The program will look for the cache in both location, and report if found."
    // We check local first.
    if (read_cache(local_cache_path, summary)) {
        g_cache_found++;
        return;
    }
    if (read_cache(export_cache_path, summary)) {
        g_cache_found++;
        return;
    }

    // Cache miss, process text file
    summary->is_constant = 0;
    summary->count = 0;
    summary->timestamps = NULL;
    summary->start = 0;
    summary->end = 0;

    FILE *fp = fopen(filepath, "r");
    if (!fp) return;

    // Use a temporary dynamic array to store timestamps
    size_t cap = 1000;
    double *ts_arr = malloc(cap * sizeof(double));
    long count = 0;

    char line[1024];
    while (fgets(line, sizeof(line), fp)) {
        if (line[0] == '#') continue;
        char *token = strtok(line, " \t");
        int col = 0;
        double timestamp = 0.0;
        int found = 0;
        while (token) {
            if (col == 4) { timestamp = atof(token); found = 1; break; }
            token = strtok(NULL, " \t");
            col++;
        }
        if (found) {
            if (count == cap) {
                cap *= 2;
                ts_arr = realloc(ts_arr, cap * sizeof(double));
            }
            ts_arr[count++] = timestamp;
        }
    }
    fclose(fp);

    summary->count = count;
    summary->timestamps = ts_arr;
    if (count > 0) {
        summary->start = ts_arr[0];
        summary->end = ts_arr[count - 1];
    }

    // Analyze for constant frame rate
    if (count > 2) {
        double dt_sum = 0;
        for (long i = 0; i < count - 1; i++) {
            dt_sum += (ts_arr[i+1] - ts_arr[i]);
        }
        double mean_dt = dt_sum / (count - 1);
        int constant = 1;
        for (long i = 0; i < count - 1; i++) {
            double dt = ts_arr[i+1] - ts_arr[i];
            if (fabs(dt - mean_dt) > 0.05 * mean_dt) {
                constant = 0;
                break;
            }
        }
        if (constant) {
            summary->is_constant = 1;
            free(summary->timestamps);
            summary->timestamps = NULL;
        }
    }

    // Write cache
    if (g_cache_export) {
        if (dir_sep) {
            char dir_path[4096];
            size_t dir_len = dir_sep - filepath;
            if (dir_len >= sizeof(dir_path)) dir_len = sizeof(dir_path) - 1;
            strncpy(dir_path, filepath, dir_len);
            dir_path[dir_len] = '\0';
            ensure_cache_dir_exists(dir_path);
        } else {
            ensure_cache_dir_exists(".");
        }
        write_cache(export_cache_path, summary);
    } else {
        ensure_path_exists(local_cache_path);
        write_cache(local_cache_path, summary);
    }
    g_cache_created++;
}

// pass 0 = count frames and populate file list, pass 1 = binning and headers (using cached files)
void scan_stream_dir(const char *path, const char *stream_name, double tstart, double tend, StreamList *streams, int num_bins, int pass, long *file_count) {
    if (pass != 0) return; // Only used for pass 0 now

    printf("Scanning %s\n", path);
    struct dirent **namelist;
    int n = scandir(path, &namelist, NULL, alphasort);
    if (n < 0) return;

    // Get date from path (parent directory name)
    // path format: .../YYYYMMDD/stream_name
    char *path_copy = strdup(path);
    // Remove trailing slash if any
    size_t p_len = strlen(path_copy);
    if (p_len > 0 && path_copy[p_len-1] == '/') path_copy[p_len-1] = '\0';

    char *last_slash = strrchr(path_copy, '/');
    char date_str[32] = {0};
    if (last_slash) {
        *last_slash = '\0'; // Terminate at stream_name slash
        char *parent_slash = strrchr(path_copy, '/');
        if (parent_slash) {
             strncpy(date_str, parent_slash + 1, 31);
        } else {
             strncpy(date_str, path_copy, 31); // No parent slash, so this is the date dir?
        }
    } else {
        // Should not happen given the structure, but fallback
        strncpy(date_str, "19700101", 31);
    }
    free(path_copy);

    // Pre-calculate timestamps for filtering
    double *timestamps = malloc(n * sizeof(double));
    for (int i = 0; i < n; i++) {
        struct dirent *dir = namelist[i];
        size_t len = strlen(dir->d_name);
        if (len > 4 && strcmp(dir->d_name + len - 4, ".txt") == 0) {
             timestamps[i] = parse_filename_time(dir->d_name, date_str);
        } else {
             timestamps[i] = 0.0;
        }
    }

    for (int i = 0; i < n; i++) {
        struct dirent *dir = namelist[i];
        if (dir->d_name[0] == '.') {
            free(namelist[i]);
            continue;
        }

        size_t len = strlen(dir->d_name);
        if (len > 4 && strcmp(dir->d_name + len - 4, ".txt") == 0) {

            double file_ts = timestamps[i];

            // Optimization: Skip files outside range
            // 1. If this file starts after tend, we can stop (assuming sorted)
            if (file_ts > tend) {
                free(namelist[i]);
                continue;
                // We could break here if we trust sort order completely and that filenames are chronological.
                // alphasort on "sname_HH..." is chronological.
                // However, there might be other streams mixed in? No, directory is per stream.
                // So break is safe.
                // But let's be safe and just continue for now or check stream name consistency?
                // The directory contains files for ONE stream: "sname".
                // So break is safe.
                // break;
            }

            // 2. If next file starts before tstart, then this file ends before tstart
            // (assuming contiguous or close files)
            int skip = 0;
            // Find next valid file timestamp
            for (int k = i + 1; k < n; k++) {
                if (timestamps[k] > 0.0) {
                    if (timestamps[k] < tstart) {
                        skip = 1;
                    }
                    break;
                }
            }

            if (skip) {
                free(namelist[i]);
                continue;
            }

            if (file_count) {
                (*file_count)++;
            }

            char filepath[1024];
            snprintf(filepath, sizeof(filepath), "%s/%s", path, dir->d_name);

            Stream *s = get_or_create_stream(streams, stream_name);
            add_file_to_stream(s, filepath, file_ts);

            // Pass 0: Count frames
            FileSummary summary;
            get_file_data(filepath, &summary);

            // If constant, we can check overlap with [tstart, tend] analytically
            if (summary.is_constant) {
                if (summary.count > 0) {
                    // Check if file range overlaps with scan range
                    if (summary.end >= tstart && summary.start <= tend) {
                        // Count frames inside [tstart, tend]
                        // Assume linear distribution
                        if (summary.start >= tstart && summary.end <= tend) {
                            s->total_frames += summary.count;
                        } else {
                            // Partial overlap
                             double dt = (summary.end - summary.start) / (summary.count > 1 ? summary.count - 1 : 1);
                             if (dt > 0) {
                                 double first_t = summary.start;
                                 long start_idx = 0;
                                 if (first_t < tstart) {
                                     start_idx = (long)ceil((tstart - first_t) / dt);
                                 }
                                 long end_idx = summary.count - 1;
                                 if (summary.end > tend) {
                                     end_idx = (long)floor((tend - first_t) / dt);
                                 }
                                 if (start_idx <= end_idx) {
                                     s->total_frames += (end_idx - start_idx + 1);
                                 }
                             } else {
                                 // Single frame
                                 if (summary.start >= tstart && summary.start <= tend) s->total_frames++;
                             }
                        }
                    }
                }
            } else {
                if (summary.timestamps) {
                    for (long k = 0; k < summary.count; k++) {
                        if (summary.timestamps[k] >= tstart && summary.timestamps[k] <= tend) {
                            s->total_frames++;
                        }
                    }
                    free(summary.timestamps);
                }
            }
        }
        free(namelist[i]);
    }
    free(timestamps);
    free(namelist);
}

void process_stream_data(StreamList *stream_list, double tstart, double tend, int num_bins) {
    for (int i = 0; i < stream_list->count; i++) {
        Stream *s = &stream_list->streams[i];

        for (int j = 0; j < s->file_count; j++) {
            char *filepath = s->files[j].path;

            // Header Scan
            if (kscan_ctx.target_key_pattern[0] != '\0') {
                int process_this = 1;
                if (kscan_ctx.target_stream[0] != '\0' && strcmp(s->name, kscan_ctx.target_stream) != 0) {
                    process_this = 0;
                }
                if (process_this) {
                    char headerpath[1024];
                    strncpy(headerpath, filepath, sizeof(headerpath));
                    headerpath[strlen(filepath) - 4] = '\0';
                    strncat(headerpath, ".fits.header", sizeof(headerpath) - strlen(headerpath) - 1);

                    FILE *fp = fopen(filepath, "r");
                    if (fp) {
                        char line[1024];
                        double file_ts = 0.0;
                        while(fgets(line, sizeof(line), fp)) {
                            if (line[0] == '#') continue;
                            char *token = strtok(line, " \t");
                            int col = 0;
                            while (token) {
                                if (col == 4) { file_ts = atof(token); break; }
                                token = strtok(NULL, " \t");
                                col++;
                            }
                            if (file_ts > 0) break;
                        }
                        fclose(fp);
                        if (file_ts >= tstart && file_ts <= tend) {
                            process_header_for_key(headerpath, s->name, file_ts);
                        }
                    }
                }
            }

            // Binning
            FileSummary summary;
            get_file_data(filepath, &summary);

            if (summary.is_constant) {
                if (summary.count > 0 && summary.end >= tstart && summary.start <= tend) {
                    double dt = (summary.end - summary.start) / (summary.count > 1 ? summary.count - 1 : 1);
                    if (dt > 0) {
                        double first_t = summary.start;
                        long start_idx = 0;
                         if (first_t < tstart) {
                             start_idx = (long)ceil((tstart - first_t) / dt);
                         }
                         long end_idx = summary.count - 1;
                         if (summary.end > tend) {
                             end_idx = (long)floor((tend - first_t) / dt);
                         }

                         for (long k = start_idx; k <= end_idx; k++) {
                             double timestamp = first_t + k * dt;
                             int bin = (int)((timestamp - tstart) / (tend - tstart) * num_bins);
                             if (bin < 0) bin = 0;
                             if (bin >= num_bins) bin = num_bins - 1;
                             s->bins[bin]++;
                             if (s->bins[bin] > s->max_bin_count) {
                                 s->max_bin_count = s->bins[bin];
                             }
                         }
                    } else {
                        // Single frame
                        if (summary.start >= tstart && summary.start <= tend) {
                             int bin = (int)((summary.start - tstart) / (tend - tstart) * num_bins);
                             if (bin < 0) bin = 0;
                             if (bin >= num_bins) bin = num_bins - 1;
                             s->bins[bin]++;
                             if (s->bins[bin] > s->max_bin_count) {
                                 s->max_bin_count = s->bins[bin];
                             }
                        }
                    }
                }
            } else {
                 if (summary.timestamps) {
                    for (long k = 0; k < summary.count; k++) {
                        double timestamp = summary.timestamps[k];
                        if (timestamp >= tstart && timestamp <= tend) {
                            int bin = (int)((timestamp - tstart) / (tend - tstart) * num_bins);
                            if (bin < 0) bin = 0;
                            if (bin >= num_bins) bin = num_bins - 1;
                            s->bins[bin]++;
                            if (s->bins[bin] > s->max_bin_count) {
                                s->max_bin_count = s->bins[bin];
                            }
                        }
                    }
                    free(summary.timestamps);
                 }
            }
        }
    }
}

void get_date_bounds(const char *root_dir, const char *date_str, double *t_min, double *t_max) {
    char date_path[1024];
    snprintf(date_path, sizeof(date_path), "%s/%s", root_dir, date_str);

    *t_min = -1.0;
    *t_max = -1.0;

    if (!is_directory(date_path)) return;

    struct dirent **streamlist;
    int n_stream = scandir(date_path, &streamlist, NULL, alphasort);
    if (n_stream < 0) return;

    for (int i = 0; i < n_stream; i++) {
        struct dirent *dir = streamlist[i];
        if (dir->d_name[0] == '.') { free(streamlist[i]); continue; }

        char stream_path[2048];
        snprintf(stream_path, sizeof(stream_path), "%s/%s", date_path, dir->d_name);

        if (is_directory(stream_path)) {
            struct dirent **filelist;
            int n_files = scandir(stream_path, &filelist, NULL, alphasort);
            if (n_files >= 0) {
                for (int j = 0; j < n_files; j++) {
                    struct dirent *fdir = filelist[j];
                    size_t len = strlen(fdir->d_name);
                    if (len > 4 && strcmp(fdir->d_name + len - 4, ".txt") == 0) {
                        char filepath[4096];
                        snprintf(filepath, sizeof(filepath), "%s/%s", stream_path, fdir->d_name);
                        FILE *fp = fopen(filepath, "r");
                        if (fp) {
                            char line[1024];
                            while (fgets(line, sizeof(line), fp)) {
                                if (line[0] == '#') continue;
                                char *token = strtok(line, " \t");
                                int col = 0;
                                double ts = 0.0;
                                int found = 0;
                                while (token) {
                                    if (col == 4) { ts = atof(token); found = 1; break; }
                                    token = strtok(NULL, " \t");
                                    col++;
                                }
                                if (found && ts > 0.0) {
                                    if (*t_min < 0 || ts < *t_min) *t_min = ts;
                                    if (*t_max < 0 || ts > *t_max) *t_max = ts;
                                }
                            }
                            fclose(fp);
                        }
                    }
                    free(filelist[j]);
                }
                free(filelist);
            }
        }
        free(streamlist[i]);
    }
    free(streamlist);
}

void process_all_dates(const char *root_dir, double tstart, double tend, StreamList *stream_list, int timeline_width, int pass, long *file_count) {
    time_t current_t = (time_t)tstart;
    time_t end_t = (time_t)tend;

    struct tm start_tm_struct;
    gmtime_r(&current_t, &start_tm_struct);
    start_tm_struct.tm_hour = 0; start_tm_struct.tm_min = 0; start_tm_struct.tm_sec = 0;
    time_t iter_t = timegm(&start_tm_struct);

    struct tm end_tm_struct;
    gmtime_r(&end_t, &end_tm_struct);
    end_tm_struct.tm_hour = 0; end_tm_struct.tm_min = 0; end_tm_struct.tm_sec = 0;
    time_t end_iter_t = timegm(&end_tm_struct);

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
            struct dirent **streamlist;
            int n_stream = scandir(date_path, &streamlist, NULL, alphasort);
            if (n_stream >= 0) {
                for (int i = 0; i < n_stream; i++) {
                    struct dirent *dir = streamlist[i];
                    if (dir->d_name[0] == '.') { free(streamlist[i]); continue; }
                    char stream_path[2048];
                    snprintf(stream_path, sizeof(stream_path), "%s/%s", date_path, dir->d_name);
                    if (is_directory(stream_path)) {
                        scan_stream_dir(stream_path, dir->d_name, tstart, tend, stream_list, timeline_width, pass, file_count);
                    }
                    free(streamlist[i]);
                }
                free(streamlist);
            }
        }
    }
}

int main(int argc, char *argv[]) {
    char *root_dir = NULL;
    char *tstart_str = NULL;
    char *tend_str = NULL;
    int auto_adjust = 0;

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
        } else if (strcmp(argv[i], "-a") == 0) {
            auto_adjust = 1;
        } else if (strcmp(argv[i], "-cacheexport") == 0) {
            g_cache_export = 1;
        } else {
            if (pos_arg_count == 0) root_dir = argv[i];
            else if (pos_arg_count == 1) tstart_str = argv[i];
            else if (pos_arg_count == 2) tend_str = argv[i];
            pos_arg_count++;
        }
    }

    if (pos_arg_count < 2 || (!auto_adjust && pos_arg_count < 3)) {
        fprintf(stderr, "Usage: %s [-k KEY] [-a] <dir> <tstart> [<tend>]\n", argv[0]);
        return 1;
    }

    double tstart = parse_time_arg(tstart_str);
    double tend = 0.0;

    if (auto_adjust) {
        // Parse date from tstart_str
        char date_str[32];
        if (strncmp(tstart_str, "UT", 2) == 0) {
             // Expect UTYYYYMMDD...
             strncpy(date_str, tstart_str + 2, 8);
             date_str[8] = '\0';
        } else {
             // If unix timestamp, convert to date
             time_t t = (time_t)tstart;
             struct tm tm_val;
             gmtime_r(&t, &tm_val);
             snprintf(date_str, sizeof(date_str), "%04d%02d%02d",
                 tm_val.tm_year + 1900, tm_val.tm_mon + 1, tm_val.tm_mday);
        }

        get_date_bounds(root_dir, date_str, &tstart, &tend);
        if (tstart < 0 || tend < 0) {
            fprintf(stderr, "Error: No data found in %s/%s to determine time range.\n", root_dir, date_str);
            return 1;
        }
    } else {
        tend = parse_time_arg(tend_str);
    }

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

    StreamList stream_list;
    init_stream_list(&stream_list);

    long file_count = 0;
    // Pass 1: Discovery and counts
    process_all_dates(root_dir, tstart, tend, &stream_list, 0, 0, &file_count);

    // Calculate formatting
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

    // Name(max) + "   " + Count(max) + "   " + " 123.4 Hz "
    int prefix_width = max_name_len + 3 + max_count_len + 3 + 10 + 1;

    int timeline_width = term_width - prefix_width - 1;
    if (timeline_width < 10) timeline_width = 10;

    // Allocate bins
    for (int i = 0; i < stream_list.count; i++) {
        stream_list.streams[i].bins = calloc(timeline_width, sizeof(int));
    }

    // Pass 2: Data processing
    process_stream_data(&stream_list, tstart, tend, timeline_width);

    // Handle end of keyword tracking
    for (int i = 0; i < kscan_ctx.tracked_count; i++) {
        TrackedKey *tk = &kscan_ctx.tracked_keys[i];
        if (tk->has_last_value) {
            ReportLine count_line;
            count_line.is_count_line = 1;
            count_line.count = tk->count_same_val;
            count_line.ts = tend;
            strncpy(count_line.keyname, tk->key, 79);
            strncpy(count_line.stream_name, tk->stream_name, 255);
            add_report_line(&kscan_ctx.report, count_line);

            ReportLine end_line;
            end_line.is_count_line = 0;
            strncpy(end_line.stream_name, tk->stream_name, 255);
            strncpy(end_line.keyname, tk->key, 79);
            end_line.ts = tend;
            strcpy(end_line.status, "END");
            strncpy(end_line.value, tk->last_value, 255);
            end_line.filename[0] = '\0';
            add_report_line(&kscan_ctx.report, end_line);
        }
    }

    // Output
    char start_str[64];
    char end_str[64];
    format_time_iso(tstart, start_str, sizeof(start_str));
    format_time_iso(tend, end_str, sizeof(end_str));

    double dt_per_char = (tend - tstart) / timeline_width;
    double duration = tend - tstart;

    printf("\nStart: %s  End: %s  Duration: %.3f s  Bin: %.3f s  Files: %ld\n\n",
           start_str, end_str, duration, dt_per_char, file_count);

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

        if (marker == 'H') {
            printf(BG_HIGHLIGHT_H "%c" RESET_COLOR, marker);
        } else if (marker == 'M') {
            printf(BG_HIGHLIGHT_M "%c" RESET_COLOR, marker);
        } else {
            putchar(marker);
        }
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
            if (idx == 0) printf("%s", BG_BLACK);
            else printf("%s", BG_SCALE);

            if (idx > 0) printf("%s", COLORS[idx]);
            printf("%s", BLOCKS[idx]);
            printf(RESET_COLOR);
        }
        printf("\n");

        // Render Keyword Timeline Row(s)
        if (kscan_ctx.target_key_pattern[0] != '\0') {
            for (int k = 0; k < kscan_ctx.tracked_count; k++) {
                TrackedKey *tk = &kscan_ctx.tracked_keys[k];
                if (strcmp(tk->stream_name, s->name) != 0) continue;

                char *key_line = malloc(timeline_width + 1);
                memset(key_line, ' ', timeline_width);
                key_line[timeline_width] = '\0';

                int has_key_entries = 0;

                // Pass 1: Pipes
                for (int r = 0; r < kscan_ctx.report.count; r++) {
                    ReportLine *l = &kscan_ctx.report.lines[r];
                    if (l->is_count_line) continue;
                    if (strcmp(l->stream_name, s->name) != 0) continue;
                    if (strcmp(l->keyname, tk->key) != 0) continue;

                    if (l->ts >= tstart && l->ts <= tend) {
                        int bin = (int)((l->ts - tstart) / (tend - tstart) * timeline_width);
                        if (bin >= 0 && bin < timeline_width) {
                            key_line[bin] = '|';
                            has_key_entries = 1;
                        }
                    }
                }

                // Pass 2: Values
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
        if (i == 0) printf("%s", BG_BLACK);
        else printf("%s", BG_SCALE);

        if (i > 0) printf("%s", COLORS[i]);
        printf("%s", BLOCKS[i]);
        printf(RESET_COLOR);
    }
    printf(" (Low -> High density)\n");

    if (kscan_ctx.report.count > 0) {
        printf("\nKeyword Scan Report:\n");
        qsort(kscan_ctx.report.lines, kscan_ctx.report.count, sizeof(ReportLine), compare_report_lines);
        for (int i = 0; i < kscan_ctx.report.count; i++) {
            ReportLine *l = &kscan_ctx.report.lines[i];
            if (l->is_count_line) {
                printf("        %d files\n", l->count);
            } else {
                char time_str[64];
                format_time_iso(l->ts, time_str, sizeof(time_str));
                printf("%-20s %-24s %-18.6f %-10s %-20s %s\n",
                       l->keyname, time_str, l->ts, l->status, l->value, l->filename);
            }
        }
    }

    free_report(&kscan_ctx.report);
    if (kscan_ctx.tracked_keys) free(kscan_ctx.tracked_keys);
    if (kscan_ctx.target_key_pattern[0] != '\0') regfree(&kscan_ctx.key_regex);
    free_stream_list(&stream_list);

    printf("\nCache: searched %ld, found %ld, created %ld\n", g_cache_searched, g_cache_found, g_cache_created);

    return 0;
}
