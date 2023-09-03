#ifndef PROTO_TEXT_H
#define PROTO_TEXT_H


//#define FORCE_HITRATIO //To test eviction overhead, force hit ratio every <settings.force_hit_ratio> get requests


/* text protocol handlers */
void complete_nread_ascii(conn *c);
int try_read_command_asciiauth(conn *c);
int try_read_command_ascii(conn *c);
void process_command_ascii(conn *c, char *command);

#endif
