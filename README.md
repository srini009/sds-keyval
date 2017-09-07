# mochi service for keyvals

- implement any old keyval in the backend
- project key-val interface out to clients
- server does the store/retrieve

## Cloning

Until RobL figures out git submodules and branch tracking, we just copied Zi Qi Wang's BwTree implementation into our tree.

## Build requirements
- the ''mercury suite'': `mercury`, `margo`, `argobots`, `abt-snoozer`, and friends. 
- autotools

## Examples

The server is pretty simple: all the RPC registration happens in `kv_server_register`, so we merely need to hang around and listen for client requests:

```C
#include <sds-keyval.h>

int main(int argc, char **argv) {
	kv_context * context = kv_server_register(argc, argv);

	margo_wait_for_finalize(context->mid);

	kv_server_deregister(context);
}
```

Both server and client include `<sds-keyval.h>`.  The server code needs to
link against `libkvserver` and all the other "mercury suite" dependencies.

Client side, all of the remote functionality is abstracted under the client API, aside from a server string passed to `kv_open`.

```C
#include <sds-keyval.h>

int main(int argc, char **argv) {
	int ret;
	kv_context * context = kv_client_register(argc, argv);

	/* open */
	ret = kv_open(context, argv[1], "booger", KV_INT, KV_INT);

	/* put */
	int key = 10;
	int val = 10;
	ret = kv_put(context, &key, &val);

	/* get */
	int remote_val;
	ret = kv_get(context, &key, &remote_val);
	printf("key: %d in: %d out: %d\n", key, val, remote_val);

	/* close */
	ret = kv_close(context);

	kv_client_deregister(context);
}
```

To compile this code, you'll need to link against `libkvclient` in addition to the "mercury suite" dependencies.

The key and value type information passed to `kv_open` might not end up being
so useful in the end.  We're still working on that one.


## Issues

If you get 
```undefined reference to `__atomic_compare_exchange_16'
```

That suggests gcc needs a bit of help pulling in the 'libatomic' library
.  The server uses CMU's BwTree implementaiton, which in turn relies on atomic operations.  Link server-side code with `libatomic` to address this issue.

## Notes
The [https://github.com/wangziqi2013/BwTree](BwTree) implementation in here
came from Zi Qi Wang's git repository.
