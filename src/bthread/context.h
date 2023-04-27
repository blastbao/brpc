/*

    libcontext - a slightly more portable version of boost::context

    Copyright Martin Husemann 2013.
    Copyright Oliver Kowalke 2009.
    Copyright Sergue E. Leontiev 2013.
    Copyright Thomas Sailer 2013.
    Minor modifications by Tomasz Wlostowski 2016.

 Distributed under the Boost Software License, Version 1.0.
      (See accompanying file LICENSE_1_0.txt or copy at
            http://www.boost.org/LICENSE_1_0.txt)

*/

#ifndef BTHREAD_CONTEXT_H
#define BTHREAD_CONTEXT_H

#include <stdint.h>
#include <stdio.h>
#include <stddef.h>

#if defined(__GNUC__) || defined(__APPLE__)

  #define BTHREAD_CONTEXT_COMPILER_gcc

  #if defined(__linux__)
	#ifdef __x86_64__
	    #define BTHREAD_CONTEXT_PLATFORM_linux_x86_64
	    #define BTHREAD_CONTEXT_CALL_CONVENTION

	#elif __i386__
	    #define BTHREAD_CONTEXT_PLATFORM_linux_i386
	    #define BTHREAD_CONTEXT_CALL_CONVENTION
	#elif __arm__
	    #define BTHREAD_CONTEXT_PLATFORM_linux_arm32
	    #define BTHREAD_CONTEXT_CALL_CONVENTION
	#elif __aarch64__
	    #define BTHREAD_CONTEXT_PLATFORM_linux_arm64
	    #define BTHREAD_CONTEXT_CALL_CONVENTION
	#endif

  #elif defined(__MINGW32__) || defined (__MINGW64__)
	#if defined(__x86_64__)
	    #define BTHREAD_CONTEXT_COMPILER_gcc
    	    #define BTHREAD_CONTEXT_PLATFORM_windows_x86_64
	    #define BTHREAD_CONTEXT_CALL_CONVENTION
	#elif defined(__i386__)
	    #define BTHREAD_CONTEXT_COMPILER_gcc
	    #define BTHREAD_CONTEXT_PLATFORM_windows_i386
	    #define BTHREAD_CONTEXT_CALL_CONVENTION __cdecl
	#endif

  #elif defined(__APPLE__) && defined(__MACH__)
	#if defined (__i386__)
	    #define BTHREAD_CONTEXT_PLATFORM_apple_i386
	    #define BTHREAD_CONTEXT_CALL_CONVENTION
	#elif defined (__x86_64__)
	    #define BTHREAD_CONTEXT_PLATFORM_apple_x86_64
	    #define BTHREAD_CONTEXT_CALL_CONVENTION
	#elif defined (__aarch64__)
	    #define BTHREAD_CONTEXT_PLATFORM_apple_arm64
	    #define BTHREAD_CONTEXT_CALL_CONVENTION
    #endif
  #endif

#endif

#if defined(_WIN32_WCE)
typedef int intptr_t;
#endif

typedef void* bthread_fcontext_t;

#ifdef __cplusplus
extern "C"{
#endif


// bthread_jump_fcontext 函数可以将当前线程的上下文（包括栈指针、寄存器等）保存到一个协程上下文结构体中，并跳转到另一个协程的上下文中，让另一个协程继续执行。
// 在这个过程中，当前协程会被挂起，等待下次被调度恢复执行。当该函数执行完成后，会再次跳转回原先的上下文中，继续执行原先协程中的代码。
//
// 该函数的声明中：
//   - ofc 表示当前协程的上下文
//   - nfc 表示目标协程的上下文
//   - vp 表示返回值
//   - preserve_fpu 表示是否需要保存浮点寄存器
//
// 在实际使用中，通常不需要直接调用 bthread_jump_fcontext 函数来切换协程，而是通过 brpc 提供的封装函数，
// 比如 sched_to、TaskGroup 等来实现协程的高级调度管理功能。
// 比如，在 brpc 框架中，我们可以使用 Bthread 类来开启协程，并使用 Bthread::suspend() 和 Bthread::resume() 等接口实现协程之间的切换。
// 同时，在使用协程时还需要注意避免出现死锁、竞态条件等常见问题，以保证程序的正确性和可靠性。
//
// bthread_jump_fcontext 的功能与系统原生的 setjmp 和 longjmp 函数类似，都是通过保存和恢复上下文环境来实现线程切换。
intptr_t BTHREAD_CONTEXT_CALL_CONVENTION
bthread_jump_fcontext(bthread_fcontext_t * ofc, bthread_fcontext_t nfc, intptr_t vp, bool preserve_fpu = false);
bthread_fcontext_t BTHREAD_CONTEXT_CALL_CONVENTION
bthread_make_fcontext(void* sp, size_t size, void (* fn)( intptr_t));

#ifdef __cplusplus
};
#endif

#endif  // BTHREAD_CONTEXT_H
