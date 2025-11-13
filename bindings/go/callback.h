#ifndef CALLBACK_H
#define CALLBACK_H

typedef int (*callback_fn_t)(const char*);
callback_fn_t get_go_callback_ptr();

#endif
