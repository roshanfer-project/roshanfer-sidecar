handle SIGSEGV stop print nopass
handle SIGABRT stop print nopass
handle SIGFPE  stop print nopass
handle SIGILL  stop print nopass
handle SIGBUS  stop print nopass


run                         

echo \n================ BACKTRACE ================\n
thread apply all bt
echo ============================================\n

generate-core-file /cores/core.%e.%p.%t