#include "_cgo_export.h"

typedef int (*callback_fn_t)(const char*);

callback_fn_t get_go_callback_ptr() {
    return (callback_fn_t)goCallback;
}
