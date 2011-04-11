#include <signal.h>
#include <cstdlib>
#include "signals.h"

static int signals[] = {SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGSEGV, SIGTERM, SIGUSR1, SIGUSR2, SIGXCPU, SIGXFSZ};
static int pending_signals[10];
static struct sigaction old_actions[10];

static void remember_signal(int signum) {
	for (unsigned int i = 0; i < sizeof(signals) / sizeof(int); ++i) {
		if(signals[i] == signum) {
			pending_signals[i] = 1;
			return;
		}
	}
}

void set_signal_handler(void(*handler)(int)) {
	struct sigaction newAction;

	//Initialize newAction
	newAction.sa_handler=handler;
	sigemptyset(&newAction.sa_mask);
	newAction.sa_flags=0;

	//Install newAction as the signal handler for all signals in the signals array
	for (unsigned int i = 0; i < sizeof(signals) / sizeof(int); ++i) {
		sigaction(signals[i], &newAction, NULL);
	}
}

void defer_signals() {
	struct sigaction newAction;

	//Set the signalsPending array to 0
	for (unsigned int i = 0; i < sizeof(signals) / sizeof(int); ++i) {
		pending_signals[i] = 0;
	}

	//Initialize newAction
	newAction.sa_handler = remember_signal;
	sigemptyset(&newAction.sa_mask);
	newAction.sa_flags = 0;

	//Install newAction as the signal handler for all signals in the signals array
	//and remember the previous signal handlers in the oldActions array
	for (unsigned int i = 0; i < sizeof(signals) / sizeof(int); ++i) {
		sigaction(signals[i], &newAction, &(old_actions[i]));
	}
}

void reset_signal_handler() {
	//Install the handlers in the oldActions array for all signals in the signals array
	for (unsigned int i = 0; i < sizeof(signals) / sizeof(int); ++i) {
		sigaction(signals[i], &(old_actions[i]), NULL);
	}

	//Raise all signals remembered in the signalsPending array
	for (unsigned int i = 0; i < sizeof(signals) / sizeof(int); ++i) {
		if (pending_signals[i]) {
			raise(signals[i]);
		}
	}
}
