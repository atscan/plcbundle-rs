#include "plcbundle.h"
#include <stddef.h>

// This function will be called by Rust, and it will call the Go function
// The Go function is exported via //export, so it's available as a C function
int exportCallbackBridge(const char* data, size_t len, void* user_data) {
    // The Go-exported function will be linked in
    extern int exportCallbackGo(const char* data, size_t len, void* user_data);
    return exportCallbackGo(data, len, user_data);
}

