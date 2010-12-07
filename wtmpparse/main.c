#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <utmp.h>

static const char *
type_to_string(short);

void
print_usage(char *name) {
    fprintf(stderr, "Usage: %s [file]\n", name);
}

void
print_entry(FILE *f, struct utmp *entry) {
    long int ip = *entry->ut_addr_v6;
    unsigned char bits[4];
    /* Print as JSON */
    fprintf(f, "{\"type\":\"%s\",", type_to_string(entry->ut_type));
    fprintf(f, "\"pid\":%d,", entry->ut_pid);
    fprintf(f, "\"device\":\"%s\",", entry->ut_line);
    fprintf(f, "\"terminal\":\"%s\",", entry->ut_id);
    fprintf(f, "\"user\":\"%s\",", entry->ut_user);
    fprintf(f, "\"host\":\"%s\",", entry->ut_host);
    fprintf(f, "\"termination\":%d,", entry->ut_exit.e_termination);
    fprintf(f, "\"exit\":%d,", entry->ut_exit.e_exit);
    fprintf(f, "\"session\":%d,", entry->ut_session);
    fprintf(f, "\"seconds\":%d,", entry->ut_tv.tv_sec);
    fprintf(f, "\"useconds\":%d,", entry->ut_tv.tv_usec);
    /* XXX Assumes ipv4 */
    bits[0] = ip & 0xFF;
    bits[1] = (ip >> 8) & 0xFF;
    bits[2] = (ip >> 16) & 0xFF;
    bits[3] = (ip >> 24) & 0xFF;
    fprintf(f, "\"ip\":\"%d.%d.%d.%d\"", bits[0], bits[1], bits[2], bits[3]);
    fprintf(f, "}\n");
}

int
main(int argc, char **argv) {
    /* The current utmp entry. */
    struct utmp *current = NULL;
    if (argc >= 2) {
        /* Parse --help or -h flag. */
        if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
            print_usage(argv[0]);
            return 1;
        }
        /* Set the utmp file. */
        utmpname(argv[1]);
    }
    /* Start parsing. */
    setutent();
    while ((current = getutent())) {
        print_entry(stdout, current);
    }
    endutent();
    return 0;
}

static const char *
type_to_string(short ut_type) {
    switch (ut_type) {
    case EMPTY:
        return "EMPTY";
    case RUN_LVL:
        return "RUN_LEVEL";
    case BOOT_TIME:
        return "BOOT_TIME";
    case NEW_TIME:
        return "NEW_TIME";
    case OLD_TIME:
        return "OLD_TIME";
    case INIT_PROCESS:
        return "INIT_PROCESS";
    case LOGIN_PROCESS:
        return "LOGIN_PROCESS";
    case USER_PROCESS:
        return "USER_PROCESS";
    case DEAD_PROCESS:
        return "DEAD_PROCESS";
    case ACCOUNTING:
        return "ACCOUNTING";
    }
    return "ERROR";
}
