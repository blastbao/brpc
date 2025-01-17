/*

    auto-generated file, do not modify!
    libcontext - a slightly more portable version of boost::context
    Copyright Martin Husemann 2013.
    Copyright Oliver Kowalke 2009.
    Copyright Sergue E. Leontiev 2013
    Copyright Thomas Sailer 2013.
    Minor modifications by Tomasz Wlostowski 2016.

 Distributed under the Boost Software License, Version 1.0.
      (See accompanying file LICENSE_1_0.txt or copy at
            http://www.boost.org/LICENSE_1_0.txt)

*/
#include "bthread/context.h"
#if defined(BTHREAD_CONTEXT_PLATFORM_windows_i386) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".p2align 4,,15\n"
".globl	_bthread_jump_fcontext\n"
".def	_bthread_jump_fcontext;	.scl	2;	.type	32;	.endef\n"
"_bthread_jump_fcontext:\n"
"    mov    0x10(%esp),%ecx\n"
"    push   %ebp\n"
"    push   %ebx\n"
"    push   %esi\n"
"    push   %edi\n"
"    mov    %fs:0x18,%edx\n"
"    mov    (%edx),%eax\n"
"    push   %eax\n"
"    mov    0x4(%edx),%eax\n"
"    push   %eax\n"
"    mov    0x8(%edx),%eax\n"
"    push   %eax\n"
"    mov    0xe0c(%edx),%eax\n"
"    push   %eax\n"
"    mov    0x10(%edx),%eax\n"
"    push   %eax\n"
"    lea    -0x8(%esp),%esp\n"
"    test   %ecx,%ecx\n"
"    je     nxt1\n"
"    stmxcsr (%esp)\n"
"    fnstcw 0x4(%esp)\n"
"nxt1:\n"
"    mov    0x30(%esp),%eax\n"
"    mov    %esp,(%eax)\n"
"    mov    0x34(%esp),%edx\n"
"    mov    0x38(%esp),%eax\n"
"    mov    %edx,%esp\n"
"    test   %ecx,%ecx\n"
"    je     nxt2\n"
"    ldmxcsr (%esp)\n"
"    fldcw  0x4(%esp)\n"
"nxt2:\n"
"    lea    0x8(%esp),%esp\n"
"    mov    %fs:0x18,%edx\n"
"    pop    %ecx\n"
"    mov    %ecx,0x10(%edx)\n"
"    pop    %ecx\n"
"    mov    %ecx,0xe0c(%edx)\n"
"    pop    %ecx\n"
"    mov    %ecx,0x8(%edx)\n"
"    pop    %ecx\n"
"    mov    %ecx,0x4(%edx)\n"
"    pop    %ecx\n"
"    mov    %ecx,(%edx)\n"
"    pop    %edi\n"
"    pop    %esi\n"
"    pop    %ebx\n"
"    pop    %ebp\n"
"    pop    %edx\n"
"    mov    %eax,0x4(%esp)\n"
"    jmp    *%edx\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_windows_i386) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".p2align 4,,15\n"
".globl	_bthread_make_fcontext\n"
".def	_bthread_make_fcontext;	.scl	2;	.type	32;	.endef\n"
"_bthread_make_fcontext:\n"
"mov    0x4(%esp),%eax\n"
"lea    -0x8(%eax),%eax\n"
"and    $0xfffffff0,%eax\n"
"lea    -0x3c(%eax),%eax\n"
"mov    0x4(%esp),%ecx\n"
"mov    %ecx,0x14(%eax)\n"
"mov    0x8(%esp),%edx\n"
"neg    %edx\n"
"lea    (%ecx,%edx,1),%ecx\n"
"mov    %ecx,0x10(%eax)\n"
"mov    %ecx,0xc(%eax)\n"
"mov    0xc(%esp),%ecx\n"
"mov    %ecx,0x2c(%eax)\n"
"stmxcsr (%eax)\n"
"fnstcw 0x4(%eax)\n"
"mov    $finish,%ecx\n"
"mov    %ecx,0x30(%eax)\n"
"mov    %fs:0x0,%ecx\n"
"walk:\n"
"mov    (%ecx),%edx\n"
"inc    %edx\n"
"je     found\n"
"dec    %edx\n"
"xchg   %edx,%ecx\n"
"jmp    walk\n"
"found:\n"
"mov    0x4(%ecx),%ecx\n"
"mov    %ecx,0x3c(%eax)\n"
"mov    $0xffffffff,%ecx\n"
"mov    %ecx,0x38(%eax)\n"
"lea    0x38(%eax),%ecx\n"
"mov    %ecx,0x18(%eax)\n"
"ret\n"
"finish:\n"
"xor    %eax,%eax\n"
"mov    %eax,(%esp)\n"
"call   _exit\n"
"hlt\n"
".def	__exit;	.scl	2;	.type	32;	.endef  \n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_windows_x86_64) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".p2align 4,,15\n"
".globl	bthread_jump_fcontext\n"
".def	bthread_jump_fcontext;	.scl	2;	.type	32;	.endef\n"
".seh_proc	bthread_jump_fcontext\n"
"bthread_jump_fcontext:\n"
".seh_endprologue\n"
"	push   %rbp\n"
"	push   %rbx\n"
"	push   %rsi\n"
"	push   %rdi\n"
"	push   %r15\n"
"	push   %r14\n"
"	push   %r13\n"
"	push   %r12\n"
"	mov    %gs:0x30,%r10\n"
"	mov    0x8(%r10),%rax\n"
"	push   %rax\n"
"	mov    0x10(%r10),%rax\n"
"	push   %rax\n"
"	mov    0x1478(%r10),%rax\n"
"	push   %rax\n"
"	mov    0x18(%r10),%rax\n"
"	push   %rax\n"
"	lea    -0xa8(%rsp),%rsp\n"
"	test   %r9,%r9\n"
"	je     nxt1\n"
"	stmxcsr 0xa0(%rsp)\n"
"	fnstcw 0xa4(%rsp)\n"
"	movaps %xmm6,(%rsp)\n"
"	movaps %xmm7,0x10(%rsp)\n"
"	movaps %xmm8,0x20(%rsp)\n"
"	movaps %xmm9,0x30(%rsp)\n"
"	movaps %xmm10,0x40(%rsp)\n"
"	movaps %xmm11,0x50(%rsp)\n"
"	movaps %xmm12,0x60(%rsp)\n"
"	movaps %xmm13,0x70(%rsp)\n"
"	movaps %xmm14,0x80(%rsp)\n"
"	movaps %xmm15,0x90(%rsp)\n"
"nxt1:\n"
"	xor    %r10,%r10\n"
"	push   %r10\n"
"	mov    %rsp,(%rcx)\n"
"	mov    %rdx,%rsp\n"
"	pop    %r10\n"
"	test   %r9,%r9\n"
"	je     nxt2\n"
"	ldmxcsr 0xa0(%rsp)\n"
"	fldcw  0xa4(%rsp)\n"
"	movaps (%rsp),%xmm6\n"
"	movaps 0x10(%rsp),%xmm7\n"
"	movaps 0x20(%rsp),%xmm8\n"
"	movaps 0x30(%rsp),%xmm9\n"
"	movaps 0x40(%rsp),%xmm10\n"
"	movaps 0x50(%rsp),%xmm11\n"
"	movaps 0x60(%rsp),%xmm12\n"
"	movaps 0x70(%rsp),%xmm13\n"
"	movaps 0x80(%rsp),%xmm14\n"
"	movaps 0x90(%rsp),%xmm15\n"
"nxt2:\n"
"	mov    $0xa8,%rcx\n"
"    test   %r10,%r10\n"
"    je     nxt3\n"
"    add    $0x8,%rcx\n"
"nxt3:\n"
"	lea    (%rsp,%rcx,1),%rsp\n"
"	mov    %gs:0x30,%r10\n"
"	pop    %rax\n"
"	mov    %rax,0x18(%r10)\n"
"	pop    %rax\n"
"	mov    %rax,0x1478(%r10)\n"
"	pop    %rax\n"
"	mov    %rax,0x10(%r10)\n"
"	pop    %rax\n"
"	mov    %rax,0x8(%r10)\n"
"	pop    %r12\n"
"	pop    %r13\n"
"	pop    %r14\n"
"	pop    %r15\n"
"	pop    %rdi\n"
"	pop    %rsi\n"
"	pop    %rbx\n"
"	pop    %rbp\n"
"	pop    %r10\n"
"	mov    %r8,%rax\n"
"	mov    %r8,%rcx\n"
"	jmpq   *%r10\n"
".seh_endproc\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_windows_x86_64) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".p2align 4,,15\n"
".globl	bthread_make_fcontext\n"
".def	bthread_make_fcontext;	.scl	2;	.type	32;	.endef\n"
".seh_proc	bthread_make_fcontext\n"
"bthread_make_fcontext:\n"
".seh_endprologue\n"
"mov    %rcx,%rax\n"
"sub    $0x28,%rax\n"
"and    $0xfffffffffffffff0,%rax\n"
"sub    $0x128,%rax\n"
"mov    %r8,0x118(%rax)\n"
"mov    %rcx,0xd0(%rax)\n"
"neg    %rdx\n"
"lea    (%rcx,%rdx,1),%rcx\n"
"mov    %rcx,0xc8(%rax)\n"
"mov    %rcx,0xc0(%rax)\n"
"stmxcsr 0xa8(%rax)\n"
"fnstcw 0xac(%rax)\n"
"leaq  finish(%rip), %rcx\n"
"mov    %rcx,0x120(%rax)\n"
"mov    $0x1,%rcx\n"
"mov    %rcx,(%rax)\n"
"retq\n"
"finish:\n"
"xor    %rcx,%rcx\n"
"callq  0x63\n"
"hlt\n"
"   .seh_endproc\n"
".def	_exit;	.scl	2;	.type	32;	.endef  \n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_linux_i386) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl bthread_jump_fcontext\n"
".align 2\n"
".type bthread_jump_fcontext,@function\n"
"bthread_jump_fcontext:\n"
"    movl  0x10(%esp), %ecx\n"
"    pushl  %ebp  \n"
"    pushl  %ebx  \n"
"    pushl  %esi  \n"
"    pushl  %edi  \n"
"    leal  -0x8(%esp), %esp\n"
"    test  %ecx, %ecx\n"
"    je  1f\n"
"    stmxcsr  (%esp)\n"
"    fnstcw  0x4(%esp)\n"
"1:\n"
"    movl  0x1c(%esp), %eax\n"
"    movl  %esp, (%eax)\n"
"    movl  0x20(%esp), %edx\n"
"    movl  0x24(%esp), %eax\n"
"    movl  %edx, %esp\n"
"    test  %ecx, %ecx\n"
"    je  2f\n"
"    ldmxcsr  (%esp)\n"
"    fldcw  0x4(%esp)\n"
"2:\n"
"    leal  0x8(%esp), %esp\n"
"    popl  %edi  \n"
"    popl  %esi  \n"
"    popl  %ebx  \n"
"    popl  %ebp  \n"
"    popl  %edx\n"
"    movl  %eax, 0x4(%esp)\n"
"    jmp  *%edx\n"
".size bthread_jump_fcontext,.-bthread_jump_fcontext\n"
".section .note.GNU-stack,\"\",%progbits\n"
".previous\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_linux_i386) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl bthread_make_fcontext\n"
".align 2\n"
".type bthread_make_fcontext,@function\n"
"bthread_make_fcontext:\n"
"    movl  0x4(%esp), %eax\n"
"    leal  -0x8(%eax), %eax\n"
"    andl  $-16, %eax\n"
"    leal  -0x20(%eax), %eax\n"
"    movl  0xc(%esp), %edx\n"
"    movl  %edx, 0x18(%eax)\n"
"    stmxcsr  (%eax)\n"
"    fnstcw  0x4(%eax)\n"
"    call  1f\n"
"1:  popl  %ecx\n"
"    addl  $finish-1b, %ecx\n"
"    movl  %ecx, 0x1c(%eax)\n"
"    ret \n"
"finish:\n"
"    call  2f\n"
"2:  popl  %ebx\n"
"    addl  $_GLOBAL_OFFSET_TABLE_+[.-2b], %ebx\n"
"    xorl  %eax, %eax\n"
"    movl  %eax, (%esp)\n"
"    call  _exit@PLT\n"
"    hlt\n"
".size bthread_make_fcontext,.-bthread_make_fcontext\n"
".section .note.GNU-stack,\"\",%progbits\n"
".previous\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_linux_x86_64) && defined(BTHREAD_CONTEXT_COMPILER_gcc)

// 首先，将当前协程的运行状态保存到当前协程的上下文中，并把目标协程的上下文加载进程序的栈空间中。
// 接着，它会检查是否需要保存和加载浮点寄存器。
// 最后，它会将当前协程的地址存储到指定的寄存器中，然后跳转到指定的协程开始执行。
//
// 这段代码使用了很多汇编语言的基本指令，比如 pushq、movq、jmp 等，
// 同时也涉及到一些系统调用，比如 stmxcsr、fnstcw、ldmxcsr、fldcw 等，
// 这些系统调用用于保存和加载浮点寄存器。
//
// 该函数通过汇编语言实现，可以提高协程切换的速度和效率。
//
//
// 在 brpc 框架中，bthread_jump_fcontext 函数用于实现协程的切换。
// 在执行这个函数时，它会保存当前协程的上下文，并将指针指向要切换到的协程的上下文，从而实现协程之间的切换。
//
// 具体原理如下：
//  协程的上下文是保存程序状态的一些寄存器和堆栈信息。在每次协程调度时，程序都需要将当前协程的上下文保存起来，并将要切换到的协程的上下文恢复。
//  bthread_jump_fcontext 函数使用了汇编语言来编写函数体，并使用了一些汇编指令来对寄存器和堆栈进行操作。具体来说，它执行了以下步骤：
//  a. 将当前协程的寄存器的值压入堆栈中，以便在之后进行恢复。
//  b. 将指针指向要切换到的协程的上下文，并将其也放到堆栈中。
//  c. 从堆栈中恢复指向要切换到的协程的上下文的指针，并跳转到该协程的代码执行。
//  d. 当下一次重新调度该协程时，bthread_jump_fcontext 函数将依然保存在该协程的堆栈中，再次执行该函数即可进行协程切换操作。
//
// 通过这种方式，bthread_jump_fcontext 函数实现了协程的切换操作。
// 同时，为了确保不会丢失标记，该函数也保存了浮点寄存器 FPU 和 MXCSR 的状态，因为这些寄存器中可能会保存有线程的临时变量和状态。
// 如果不正确地保存和加载这些寄存器，就会导致程序出错。

// bthread_jump_fcontext 函数使用了 jmp，但是它是协程切换的一部分，而不是函数返回的一部分。
// 调用该函数会将控制权转移到另一个协程，并且可以在后续的时间点返回到原始协程。
//
// bthread_jump_fcontext 该函数只是实现了协程之间的切换，当切换回该协程时，该函数会继续执行未完成的代码，直到执行完毕才能正常返回。
//
// 调用 bthread_jump_fcontext 函数并不意味着会一直停留在该函数中，而是通过 jmp 指令实现协程之间的切换。
// 当切换回该协程时，该函数会继续执行未完成的代码，直到执行完毕才能正常返回。
// 因此，bthread_jump_fcontext 函数会在切换回该协程之后返回并从调用处继续执行。
//
// 在brpc框架中，bthread_jump_fcontext函数用于协程的切换，并且它内部确实使用了jmp语句实现协程的切换功能。
// 但是调用该函数并不意味着永远不会返回，因为它只是将控制权切换到另一个协程中，当切换回该协程时，会继续执行之前未完成的代码。
// 所以，在正确的使用情况下，调用该函数仍然能够正常返回。
//
// 需要注意的是，jmp指令的作用是直接修改程序计数器（PC），从而改变程序流程和执行路径。
// 在 bthread_jump_fcontext 函数内部，它使用jmp指令实现了堆栈和寄存器环境的转移，以及协程之间的切换。
// 当jmp指令执行完成后，CPU会根据新的PC指向的地址开始执行代码段中的下一条指令。
// 实际上，在协程切换期间，它可以被多次调用，并且每次调用都会通过jmp指令将控制权传递给目标协程。
__asm (
".text\n"
".globl bthread_jump_fcontext\n"
".type bthread_jump_fcontext,@function\n"
".align 16\n"
"bthread_jump_fcontext:\n"
// 将 %rbp, %rbx, %r15, %r14, %r13, %r12 依次压入堆栈中，并将栈顶指针减去 8。
"    pushq  %rbp  \n"
"    pushq  %rbx  \n"
"    pushq  %r15  \n"
"    pushq  %r14  \n"
"    pushq  %r13  \n"
"    pushq  %r12  \n"
"    leaq  -0x8(%rsp), %rsp\n"
// 比较 %rcx 和 0 的值，如果相等则跳转到标号 1，否则继续执行下一条指令。
"    cmp  $0, %rcx\n"       // 判断是否有设置第四个参数
"    je  1f\n"              // 如果第四个参数为 0 ，跳过
"    stmxcsr  (%rsp)\n"
"    fnstcw   0x4(%rsp)\n"
"1:\n"
// 在内存地址 %rdi 处存储当前线程的上下文，将 %rsp 的值赋给 %rsi。
"    movq  %rsp, (%rdi)\n"  // 将当前栈顶指针保存到 from->context
"    movq  %rsi, %rsp\n"    // 将当前栈顶设置为 to->context
// 再次比较 %rcx 和 0 的值，如果相等则跳转到标号 2，否则继续执行下一条指令。
"    cmp  $0, %rcx\n"
"    je  2f\n"
// 从内存地址 %rsp 处读取保存的 MXCSR 状态并将其加载到寄存器中，从内存地址 0x4(%rsp) 处读取保存的 FPU 控制字并将其加载到 FPU 控制寄存器中。
"    ldmxcsr  (%rsp)\n"
"    fldcw  0x4(%rsp)\n"
"2:\n"
// 将栈顶指针加上 8，并依次将 %r12, %r13, %r14, %r15, %rbx, %rbp 出栈。
"    leaq  0x8(%rsp), %rsp\n"
"    popq  %r12  \n"
"    popq  %r13  \n"
"    popq  %r14  \n"
"    popq  %r15  \n"
"    popq  %rbx  \n"
"    popq  %rbp  \n"
"    popq  %r8\n"
// 将 %rdx 的值复制给 %rax 和 %rdi。
"    movq  %rdx, %rax\n"
"    movq  %rdx, %rdi\n"
// 使用 %r8 寄存器中保存的跳转地址来跳转到新的上下文中继续执行函数。
"    jmp  *%r8\n"
// 使用 .size 和 .section 等指令来定义函数的大小和 ELF 节信息，并将其与其他段进行合并。
".size bthread_jump_fcontext,.-bthread_jump_fcontext\n"
".section .note.GNU-stack,\"\",%progbits\n"
".previous\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_linux_x86_64) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl bthread_make_fcontext\n"
".type bthread_make_fcontext,@function\n"
".align 16\n"
"bthread_make_fcontext:\n"
"    movq  %rdi, %rax\n"
"    andq  $-16, %rax\n"
"    leaq  -0x48(%rax), %rax\n"
"    movq  %rdx, 0x38(%rax)\n"
"    stmxcsr  (%rax)\n"
"    fnstcw   0x4(%rax)\n"
"    leaq  finish(%rip), %rcx\n"
"    movq  %rcx, 0x40(%rax)\n"
"    ret \n"
"finish:\n"
"    xorq  %rdi, %rdi\n"
"    call  _exit@PLT\n"
"    hlt\n"
".size bthread_make_fcontext,.-bthread_make_fcontext\n"
".section .note.GNU-stack,\"\",%progbits\n"
".previous\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_apple_x86_64) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl _bthread_jump_fcontext\n"
".align 8\n"
"_bthread_jump_fcontext:\n"
"    pushq  %rbp  \n"
"    pushq  %rbx  \n"
"    pushq  %r15  \n"
"    pushq  %r14  \n"
"    pushq  %r13  \n"
"    pushq  %r12  \n"
"    leaq  -0x8(%rsp), %rsp\n"
"    cmp  $0, %rcx\n"
"    je  1f\n"
"    stmxcsr  (%rsp)\n"
"    fnstcw   0x4(%rsp)\n"
"1:\n"
"    movq  %rsp, (%rdi)\n"
"    movq  %rsi, %rsp\n"
"    cmp  $0, %rcx\n"
"    je  2f\n"
"    ldmxcsr  (%rsp)\n"
"    fldcw  0x4(%rsp)\n"
"2:\n"
"    leaq  0x8(%rsp), %rsp\n"
"    popq  %r12  \n"
"    popq  %r13  \n"
"    popq  %r14  \n"
"    popq  %r15  \n"
"    popq  %rbx  \n"
"    popq  %rbp  \n"
"    popq  %r8\n"
"    movq  %rdx, %rax\n"
"    movq  %rdx, %rdi\n"
"    jmp  *%r8\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_apple_x86_64) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl _bthread_make_fcontext\n"
".align 8\n"
"_bthread_make_fcontext:\n"
"    movq  %rdi, %rax\n"
"    movabs  $-16,           %r8\n"
"    andq    %r8,            %rax\n"
"    leaq  -0x48(%rax), %rax\n"
"    movq  %rdx, 0x38(%rax)\n"
"    stmxcsr  (%rax)\n"
"    fnstcw   0x4(%rax)\n"
"    leaq  finish(%rip), %rcx\n"
"    movq  %rcx, 0x40(%rax)\n"
"    ret \n"
"finish:\n"
"    xorq  %rdi, %rdi\n"
"    call  __exit\n"
"    hlt\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_apple_i386) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl _bthread_jump_fcontext\n"
".align 2\n"
"_bthread_jump_fcontext:\n"
"    movl  0x10(%esp), %ecx\n"
"    pushl  %ebp  \n"
"    pushl  %ebx  \n"
"    pushl  %esi  \n"
"    pushl  %edi  \n"
"    leal  -0x8(%esp), %esp\n"
"    test  %ecx, %ecx\n"
"    je  1f\n"
"    stmxcsr  (%esp)\n"
"    fnstcw  0x4(%esp)\n"
"1:\n"
"    movl  0x1c(%esp), %eax\n"
"    movl  %esp, (%eax)\n"
"    movl  0x20(%esp), %edx\n"
"    movl  0x24(%esp), %eax\n"
"    movl  %edx, %esp\n"
"    test  %ecx, %ecx\n"
"    je  2f\n"
"    ldmxcsr  (%esp)\n"
"    fldcw  0x4(%esp)\n"
"2:\n"
"    leal  0x8(%esp), %esp\n"
"    popl  %edi  \n"
"    popl  %esi  \n"
"    popl  %ebx  \n"
"    popl  %ebp  \n"
"    popl  %edx\n"
"    movl  %eax, 0x4(%esp)\n"
"    jmp  *%edx\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_apple_i386) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl _bthread_make_fcontext\n"
".align 2\n"
"_bthread_make_fcontext:\n"
"    movl  0x4(%esp), %eax\n"
"    leal  -0x8(%eax), %eax\n"
"    andl  $-16, %eax\n"
"    leal  -0x20(%eax), %eax\n"
"    movl  0xc(%esp), %edx\n"
"    movl  %edx, 0x18(%eax)\n"
"    stmxcsr  (%eax)\n"
"    fnstcw  0x4(%eax)\n"
"    call  1f\n"
"1:  popl  %ecx\n"
"    addl  $finish-1b, %ecx\n"
"    movl  %ecx, 0x1c(%eax)\n"
"    ret \n"
"finish:\n"
"    xorl  %eax, %eax\n"
"    movl  %eax, (%esp)\n"
"    call  __exit\n"
"    hlt\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_linux_arm32) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl bthread_jump_fcontext\n"
".align 2\n"
".type bthread_jump_fcontext,%function\n"
"bthread_jump_fcontext:\n"
"    @ save LR as PC\n"
"    push {lr}\n"
"    @ save V1-V8,LR\n"
"    push {v1-v8,lr}\n"
"    @ prepare stack for FPU\n"
"    sub  sp, sp, #64\n"
"    @ test if fpu env should be preserved\n"
"    cmp  a4, #0\n"
"    beq  1f\n"
"    @ save S16-S31\n"
"    vstmia  sp, {d8-d15}\n"
"1:\n"
"    @ store RSP (pointing to context-data) in A1\n"
"    str  sp, [a1]\n"
"    @ restore RSP (pointing to context-data) from A2\n"
"    mov  sp, a2\n"
"    @ test if fpu env should be preserved\n"
"    cmp  a4, #0\n"
"    beq  2f\n"
"    @ restore S16-S31\n"
"    vldmia  sp, {d8-d15}\n"
"2:\n"
"    @ prepare stack for FPU\n"
"    add  sp, sp, #64\n"
"    @ use third arg as return value after jump\n"
"    @ and as first arg in context function\n"
"    mov  a1, a3\n"
"    @ restore v1-V8,LR,PC\n"
"    pop {v1-v8,lr}\n"
"    pop {pc}\n"
".size bthread_jump_fcontext,.-bthread_jump_fcontext\n"
"@ Mark that we don't need executable stack.\n"
".section .note.GNU-stack,\"\",%progbits\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_linux_arm32) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl bthread_make_fcontext\n"
".align 2\n"
".type bthread_make_fcontext,%function\n"
"bthread_make_fcontext:\n"
"    @ shift address in A1 to lower 16 byte boundary\n"
"    bic  a1, a1, #15\n"
"    @ reserve space for context-data on context-stack\n"
"    sub  a1, a1, #104\n"
"    @ third arg of bthread_make_fcontext() == address of context-function\n"
"    str  a3, [a1,#100]\n"
"    @ compute abs address of label finish\n"
"    adr  a2, finish\n"
"    @ save address of finish as return-address for context-function\n"
"    @ will be entered after context-function returns\n"
"    str  a2, [a1,#96]\n"
"    bx  lr @ return pointer to context-data\n"
"finish:\n"
"    @ exit code is zero\n"
"    mov  a1, #0\n"
"    @ exit application\n"
"    bl  _exit@PLT\n"
".size bthread_make_fcontext,.-bthread_make_fcontext\n"
"@ Mark that we don't need executable stack.\n"
".section .note.GNU-stack,\"\",%progbits\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_linux_arm64) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".cpu    generic+fp+simd\n"
".text\n"
".align  2\n"
".global bthread_jump_fcontext\n"
".type   bthread_jump_fcontext, %function\n"
"bthread_jump_fcontext:\n"
"    # prepare stack for GP + FPU\n"
"    sub  sp, sp, #0xb0\n"
"# Because gcc may save integer registers in fp registers across a\n"
"# function call we cannot skip saving the fp registers.\n"
"#\n"
"# Do not reinstate this test unless you fully understand what you\n"
"# are doing.\n"
"#\n"
"#    # test if fpu env should be preserved\n"
"#    cmp  w3, #0\n"
"#    b.eq  1f\n"
"    # save d8 - d15\n"
"    stp  d8,  d9,  [sp, #0x00]\n"
"    stp  d10, d11, [sp, #0x10]\n"
"    stp  d12, d13, [sp, #0x20]\n"
"    stp  d14, d15, [sp, #0x30]\n"
"1:\n"
"    # save x19-x30\n"
"    stp  x19, x20, [sp, #0x40]\n"
"    stp  x21, x22, [sp, #0x50]\n"
"    stp  x23, x24, [sp, #0x60]\n"
"    stp  x25, x26, [sp, #0x70]\n"
"    stp  x27, x28, [sp, #0x80]\n"
"    stp  x29, x30, [sp, #0x90]\n"
"    # save LR as PC\n"
"    str  x30, [sp, #0xa0]\n"
"    # store RSP (pointing to context-data) in first argument (x0).\n"
"    # STR cannot have sp as a target register\n"
"    mov  x4, sp\n"
"    str  x4, [x0]\n"
"    # restore RSP (pointing to context-data) from A2 (x1)\n"
"    mov  sp, x1\n"
"#    # test if fpu env should be preserved\n"
"#    cmp  w3, #0\n"
"#    b.eq  2f\n"
"    # load d8 - d15\n"
"    ldp  d8,  d9,  [sp, #0x00]\n"
"    ldp  d10, d11, [sp, #0x10]\n"
"    ldp  d12, d13, [sp, #0x20]\n"
"    ldp  d14, d15, [sp, #0x30]\n"
"2:\n"
"    # load x19-x30\n"
"    ldp  x19, x20, [sp, #0x40]\n"
"    ldp  x21, x22, [sp, #0x50]\n"
"    ldp  x23, x24, [sp, #0x60]\n"
"    ldp  x25, x26, [sp, #0x70]\n"
"    ldp  x27, x28, [sp, #0x80]\n"
"    ldp  x29, x30, [sp, #0x90]\n"
"    # use third arg as return value after jump\n"
"    # and as first arg in context function\n"
"    mov  x0, x2\n"
"    # load pc\n"
"    ldr  x4, [sp, #0xa0]\n"
"    # restore stack from GP + FPU\n"
"    add  sp, sp, #0xb0\n"
"    ret x4\n"
".size   bthread_jump_fcontext,.-bthread_jump_fcontext\n"
"# Mark that we don't need executable stack.\n"
".section .note.GNU-stack,\"\",%progbits\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_linux_arm64) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".cpu    generic+fp+simd\n"
".text\n"
".align  2\n"
".global bthread_make_fcontext\n"
".type   bthread_make_fcontext, %function\n"
"bthread_make_fcontext:\n"
"    # shift address in x0 (allocated stack) to lower 16 byte boundary\n"
"    and x0, x0, ~0xF\n"
"    # reserve space for context-data on context-stack\n"
"    sub  x0, x0, #0xb0\n"
"    # third arg of bthread_make_fcontext() == address of context-function\n"
"    # store address as a PC to jump in\n"
"    str  x2, [x0, #0xa0]\n"
"    # save address of finish as return-address for context-function\n"
"    # will be entered after context-function returns (LR register)\n"
"    adr  x1, finish\n"
"    str  x1, [x0, #0x98]\n"
"    ret  x30 \n"
"finish:\n"
"    # exit code is zero\n"
"    mov  x0, #0\n"
"    # exit application\n"
"    bl  _exit\n"
".size   bthread_make_fcontext,.-bthread_make_fcontext\n"
"# Mark that we don't need executable stack.\n"
".section .note.GNU-stack,\"\",%progbits\n"
);

#endif


#if defined(BTHREAD_CONTEXT_PLATFORM_apple_arm64) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl _bthread_jump_fcontext\n"
".balign 16\n"
"_bthread_jump_fcontext:\n"
"    ; prepare stack for GP + FPU\n"
"    sub  sp, sp, #0xb0\n"
"#if (defined(__VFP_FP__) && !defined(__SOFTFP__))\n"
"    ; test if fpu env should be preserved\n"
"    cmp  w3, #0\n"
"    b.eq  1f\n"
"    ; save d8 - d15\n"
"    stp  d8,  d9,  [sp, #0x00]\n"
"    stp  d10, d11, [sp, #0x10]\n"
"    stp  d12, d13, [sp, #0x20]\n"
"    stp  d14, d15, [sp, #0x30]\n"
"1:\n"
"#endif\n"
"    ; save x19-x30\n"
"    stp  x19, x20, [sp, #0x40]\n"
"    stp  x21, x22, [sp, #0x50]\n"
"    stp  x23, x24, [sp, #0x60]\n"
"    stp  x25, x26, [sp, #0x70]\n"
"    stp  x27, x28, [sp, #0x80]\n"
"    stp  fp,  lr,  [sp, #0x90]\n"
"    ; save LR as PC\n"
"    str  lr, [sp, #0xa0]\n"
"    ; store RSP (pointing to context-data) in first argument (x0).\n"
"    ; STR cannot have sp as a target register\n"
"    mov  x4, sp\n"
"    str  x4, [x0]\n"
"    ; restore RSP (pointing to context-data) from A2 (x1)\n"
"    mov  sp, x1\n"
"#if (defined(__VFP_FP__) && !defined(__SOFTFP__))\n"
"    ; test if fpu env should be preserved\n"
"    cmp  w3, #0\n"
"    b.eq  2f\n"
"    ; load d8 - d15\n"
"    ldp  d8,  d9,  [sp, #0x00]\n"
"    ldp  d10, d11, [sp, #0x10]\n"
"    ldp  d12, d13, [sp, #0x20]\n"
"    ldp  d14, d15, [sp, #0x30]\n"
"2:\n"
"#endif\n"
"    ; load x19-x30\n"
"    ldp  x19, x20, [sp, #0x40]\n"
"    ldp  x21, x22, [sp, #0x50]\n"
"    ldp  x23, x24, [sp, #0x60]\n"
"    ldp  x25, x26, [sp, #0x70]\n"
"    ldp  x27, x28, [sp, #0x80]\n"
"    ldp  fp,  lr,  [sp, #0x90]\n"
"    ; use third arg as return value after jump\n"
"    ; and as first arg in context function\n"
"    mov  x0, x2\n"
"    ; load pc\n"
"    ldr  x4, [sp, #0xa0]\n"
"    ; restore stack from GP + FPU\n"
"    add  sp, sp, #0xb0\n"
"    ret x4\n"
);

#endif

#if defined(BTHREAD_CONTEXT_PLATFORM_apple_arm64) && defined(BTHREAD_CONTEXT_COMPILER_gcc)
__asm (
".text\n"
".globl _bthread_make_fcontext\n"
".balign 16\n"
"_bthread_make_fcontext:\n"
"    ; shift address in x0 (allocated stack) to lower 16 byte boundary\n"
"    and x0, x0, ~0xF\n"
"    ; reserve space for context-data on context-stack\n"
"    sub  x0, x0, #0xb0\n"
"    ; third arg of make_fcontext() == address of context-function\n"
"    ; store address as a PC to jump in\n"
"    str  x2, [x0, #0xa0]\n"
"    ; compute abs address of label finish\n"
"    ; 0x0c = 3 instructions * size (4) before label 'finish'\n"
"    ; TODO: Numeric offset since llvm still does not support labels in ADR. Fix:\n"
"    ;       http:\n"
"    adr  x1, 0x0c\n"
"    ; save address of finish as return-address for context-function\n"
"    ; will be entered after context-function returns (LR register)\n"
"    str  x1, [x0, #0x98]\n"
"    ret  lr ; return pointer to context-data (x0)\n"
"finish:\n"
"    ; exit code is zero\n"
"    mov  x0, #0\n"
"    ; exit application\n"
"    bl  __exit\n"
);

#endif

