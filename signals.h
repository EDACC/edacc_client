#ifndef __signals_h__
#define __signals_h__

extern void set_signal_handler(void (*handler)(int));
extern void defer_signals();
extern void reset_signal_handler();

#endif
