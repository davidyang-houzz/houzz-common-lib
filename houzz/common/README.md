# Howtos
## shutdown_handlers
usages:
```
shutdown_handler.init_shutdown_handler(signals)
shutdown_handler.add_shutdown_handler(handler, scope)
shutdown_handler.remove_shutdown_handlers(scope):
```

shutdown_handler is the common way to handle the signal of termination.
application who need to do cleanup on the exit, can use it to specify the cleanup handler.
by default the scope is root, all the scopes will also be run before the application exit. 
If you have any handlers don't need to run before application exit, you can put them into the 'A' scope and clean the scope after it.
