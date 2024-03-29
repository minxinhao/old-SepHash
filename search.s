	.file	"search.cc"
	.text
	.p2align 4
	.globl	_Z13linear_searchPKmim
	.type	_Z13linear_searchPKmim, @function
_Z13linear_searchPKmim:
.LFB7864:
	.cfi_startproc
	endbr64
	movslq	%esi, %rsi
	testq	%rsi, %rsi
	jle	.L5
	xorl	%eax, %eax
	jmp	.L4
	.p2align 4,,10
	.p2align 3
.L10:
	incq	%rax
	cmpq	%rsi, %rax
	je	.L9
.L4:
	cmpq	%rdx, (%rdi,%rax,8)
	jne	.L10
	ret
	.p2align 4,,10
	.p2align 3
.L9:
	movl	$-1, %eax
	ret
.L5:
	movl	$-1, %eax
	ret
	.cfi_endproc
.LFE7864:
	.size	_Z13linear_searchPKmim, .-_Z13linear_searchPKmim
	.p2align 4
	.globl	_Z21linear_search_bitmaskPKmimm
	.type	_Z21linear_search_bitmaskPKmimm, @function
_Z21linear_search_bitmaskPKmimm:
.LFB7865:
	.cfi_startproc
	endbr64
	movslq	%esi, %rsi
	testq	%rsi, %rsi
	jle	.L15
	xorl	%eax, %eax
	jmp	.L14
	.p2align 4,,10
	.p2align 3
.L19:
	incq	%rax
	cmpq	%rsi, %rax
	je	.L18
.L14:
	movq	(%rdi,%rax,8), %r8
	andq	%rcx, %r8
	cmpq	%rdx, %r8
	jne	.L19
	ret
	.p2align 4,,10
	.p2align 3
.L18:
	movl	$-1, %eax
	ret
.L15:
	movl	$-1, %eax
	ret
	.cfi_endproc
.LFE7865:
	.size	_Z21linear_search_bitmaskPKmimm, .-_Z21linear_search_bitmaskPKmimm
	.section	.rodata.str1.1,"aMS",@progbits,1
.LC0:
	.string	"%lx  "
	.text
	.p2align 4
	.globl	_Z9print_256Dv4_x
	.type	_Z9print_256Dv4_x, @function
_Z9print_256Dv4_x:
.LFB7866:
	.cfi_startproc
	endbr64
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset 6, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register 6
	pushq	%r13
	pushq	%r12
	.cfi_offset 13, -24
	.cfi_offset 12, -32
	leaq	.LC0(%rip), %r12
	pushq	%rbx
	andq	$-32, %rsp
	subq	$32, %rsp
	.cfi_offset 3, -40
	movq	%rsp, %rbx
	leaq	32(%rsp), %r13
	vmovdqa64	%ymm0, (%rsp)
	vzeroupper
.L21:
	movq	(%rbx), %rdx
	movq	%r12, %rsi
	movl	$1, %edi
	xorl	%eax, %eax
	addq	$8, %rbx
	call	__printf_chk@PLT
	cmpq	%r13, %rbx
	jne	.L21
	movl	$10, %edi
	call	putchar@PLT
	leaq	-24(%rbp), %rsp
	popq	%rbx
	popq	%r12
	popq	%r13
	popq	%rbp
	.cfi_def_cfa 7, 8
	ret
	.cfi_endproc
.LFE7866:
	.size	_Z9print_256Dv4_x, .-_Z9print_256Dv4_x
	.p2align 4
	.globl	_Z17linear_search_avxPKmim
	.type	_Z17linear_search_avxPKmim, @function
_Z17linear_search_avxPKmim:
.LFB7867:
	.cfi_startproc
	endbr64
	testl	%esi, %esi
	leal	7(%rsi), %r9d
	cmovns	%esi, %r9d
	vpbroadcastq	%rdx, %ymm1
	andl	$-8, %r9d
	movslq	%r9d, %r9
	xorl	%eax, %eax
	testq	%r9, %r9
	jne	.L25
	jmp	.L30
	.p2align 4,,10
	.p2align 3
.L28:
	testl	%r8d, %r8d
	jne	.L37
	addq	$8, %rax
	cmpq	%rax, %r9
	jbe	.L30
.L25:
	vpcmpeqq	(%rdi,%rax,8), %ymm1, %ymm0
	vpmovmskb	%ymm0, %ecx
	vpcmpeqq	32(%rdi,%rax,8), %ymm1, %ymm0
	vpmovmskb	%ymm0, %r8d
	testl	%ecx, %ecx
	je	.L28
	tzcntl	%ecx, %ecx
	sarl	$3, %ecx
	addl	%ecx, %eax
.L36:
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L30:
	movslq	%esi, %rsi
	cmpq	%rsi, %r9
	jb	.L27
	jmp	.L38
	.p2align 4,,10
	.p2align 3
.L31:
	incq	%r9
	cmpq	%rsi, %r9
	jnb	.L39
.L27:
	cmpq	%rdx, (%rdi,%r9,8)
	jne	.L31
	movl	%r9d, %eax
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L37:
	tzcntl	%r8d, %r8d
	sarl	$3, %r8d
	leal	4(%r8,%rax), %eax
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L39:
	movl	$-1, %eax
	vzeroupper
	ret
.L38:
	movl	$-1, %eax
	jmp	.L36
	.cfi_endproc
.LFE7867:
	.size	_Z17linear_search_avxPKmim, .-_Z17linear_search_avxPKmim
	.p2align 4
	.globl	_Z20linear_search_avx_16PKmim
	.type	_Z20linear_search_avx_16PKmim, @function
_Z20linear_search_avx_16PKmim:
.LFB7868:
	.cfi_startproc
	endbr64
	testl	%esi, %esi
	leal	15(%rsi), %r11d
	cmovns	%esi, %r11d
	vpbroadcastq	%rdx, %ymm1
	andl	$-16, %r11d
	movslq	%r11d, %r11
	xorl	%eax, %eax
	testq	%r11, %r11
	jne	.L41
	jmp	.L48
	.p2align 4,,10
	.p2align 3
.L44:
	testl	%r8d, %r8d
	jne	.L55
	testl	%r9d, %r9d
	jne	.L56
	testl	%r10d, %r10d
	jne	.L57
	addq	$16, %rax
	cmpq	%rax, %r11
	jbe	.L48
.L41:
	vpcmpeqq	(%rdi,%rax,8), %ymm1, %ymm0
	vpmovmskb	%ymm0, %ecx
	vpcmpeqq	32(%rdi,%rax,8), %ymm1, %ymm0
	vpmovmskb	%ymm0, %r8d
	vpcmpeqq	64(%rdi,%rax,8), %ymm1, %ymm0
	vpmovmskb	%ymm0, %r9d
	vpcmpeqq	96(%rdi,%rax,8), %ymm1, %ymm0
	vpmovmskb	%ymm0, %r10d
	testl	%ecx, %ecx
	je	.L44
	tzcntl	%ecx, %ecx
	sarl	$3, %ecx
	addl	%ecx, %eax
.L54:
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L48:
	movslq	%esi, %rsi
	cmpq	%rsi, %r11
	jb	.L43
	movl	$-1, %eax
	jmp	.L54
	.p2align 4,,10
	.p2align 3
.L49:
	incq	%r11
	cmpq	%rsi, %r11
	jnb	.L58
.L43:
	cmpq	%rdx, (%rdi,%r11,8)
	jne	.L49
	movl	%r11d, %eax
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L55:
	tzcntl	%r8d, %r8d
	sarl	$3, %r8d
	leal	4(%r8,%rax), %eax
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L56:
	tzcntl	%r9d, %r9d
	sarl	$3, %r9d
	leal	8(%r9,%rax), %eax
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L57:
	tzcntl	%r10d, %r10d
	sarl	$3, %r10d
	leal	12(%r10,%rax), %eax
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L58:
	movl	$-1, %eax
	jmp	.L54
	.cfi_endproc
.LFE7868:
	.size	_Z20linear_search_avx_16PKmim, .-_Z20linear_search_avx_16PKmim
	.p2align 4
	.globl	_Z20linear_search_avx_urPKmim
	.type	_Z20linear_search_avx_urPKmim, @function
_Z20linear_search_avx_urPKmim:
.LFB7869:
	.cfi_startproc
	endbr64
	vpbroadcastq	%rdx, %ymm1
	vpcmpeqq	(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L127
	vpcmpeqq	32(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L128
	vpcmpeqq	64(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L129
	vpcmpeqq	96(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L130
	vpcmpeqq	128(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L131
	vpcmpeqq	160(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L132
	vpcmpeqq	192(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L133
	vpcmpeqq	224(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L134
	vpcmpeqq	256(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L135
	vpcmpeqq	288(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L136
	vpcmpeqq	320(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L137
	vpcmpeqq	352(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L138
	vpcmpeqq	384(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L139
	vpcmpeqq	416(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L140
	vpcmpeqq	448(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L141
	vpcmpeqq	480(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L142
	vpcmpeqq	512(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L143
	vpcmpeqq	544(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L144
	vpcmpeqq	576(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L145
	vpcmpeqq	608(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L146
	vpcmpeqq	640(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L147
	vpcmpeqq	672(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L148
	vpcmpeqq	704(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L149
	vpcmpeqq	736(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L150
	vpcmpeqq	768(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L151
	vpcmpeqq	800(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L152
	vpcmpeqq	832(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L153
	vpcmpeqq	864(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L154
	vpcmpeqq	896(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L155
	vpcmpeqq	928(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L156
	vpcmpeqq	960(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L157
	vpcmpeqq	992(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L158
	vpcmpeqq	1024(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L159
	vpcmpeqq	1056(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L160
	vpcmpeqq	1088(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L161
	vpcmpeqq	1120(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L162
	vpcmpeqq	1152(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L163
	vpcmpeqq	1184(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L164
	vpcmpeqq	1216(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L165
	vpcmpeqq	1248(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L166
	vpcmpeqq	1280(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L167
	vpcmpeqq	1312(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L168
	vpcmpeqq	1344(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L169
	vpcmpeqq	1376(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L170
	vpcmpeqq	1408(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L171
	vpcmpeqq	1440(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%eax, %eax
	jne	.L172
	vpcmpeqq	1472(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %ecx
	vpcmpeqq	1504(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%ecx, %ecx
	jne	.L173
	testl	%eax, %eax
	jne	.L174
	vpcmpeqq	1536(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %ecx
	vpcmpeqq	1568(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%ecx, %ecx
	jne	.L175
	testl	%eax, %eax
	jne	.L176
	vpcmpeqq	1600(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %ecx
	vpcmpeqq	1632(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%ecx, %ecx
	jne	.L177
	testl	%eax, %eax
	jne	.L178
	vpcmpeqq	1664(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %ecx
	vpcmpeqq	1696(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%ecx, %ecx
	jne	.L179
	testl	%eax, %eax
	jne	.L180
	vpcmpeqq	1728(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %ecx
	vpcmpeqq	1760(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%ecx, %ecx
	jne	.L181
	testl	%eax, %eax
	jne	.L182
	vpcmpeqq	1792(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %ecx
	vpcmpeqq	1824(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%ecx, %ecx
	jne	.L183
	testl	%eax, %eax
	jne	.L184
	vpcmpeqq	1856(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %ecx
	vpcmpeqq	1888(%rdi), %ymm1, %ymm0
	vpmovmskb	%ymm0, %eax
	testl	%ecx, %ecx
	jne	.L185
	testl	%eax, %eax
	jne	.L186
	vpcmpeqq	1920(%rdi), %ymm1, %ymm0
	vpcmpeqq	1952(%rdi), %ymm1, %ymm1
	vpmovmskb	%ymm0, %ecx
	vpmovmskb	%ymm1, %eax
	testl	%ecx, %ecx
	jne	.L187
	testl	%eax, %eax
	jne	.L188
	movl	$248, %eax
	movslq	%esi, %rsi
.L122:
	cmpq	%rax, %rsi
	jle	.L125
	cmpq	%rdx, (%rdi,%rax,8)
	je	.L126
	incq	%rax
	jmp	.L122
	.p2align 4,,10
	.p2align 3
.L128:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$4, %eax
.L126:
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L127:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L130:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$12, %eax
	jmp	.L126
	.p2align 4,,10
	.p2align 3
.L129:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$8, %eax
	vzeroupper
	ret
	.p2align 4,,10
	.p2align 3
.L131:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$16, %eax
	jmp	.L126
	.p2align 4,,10
	.p2align 3
.L134:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$28, %eax
	jmp	.L126
	.p2align 4,,10
	.p2align 3
.L132:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$20, %eax
	jmp	.L126
	.p2align 4,,10
	.p2align 3
.L133:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$24, %eax
	jmp	.L126
	.p2align 4,,10
	.p2align 3
.L135:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$32, %eax
	jmp	.L126
.L136:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$36, %eax
	jmp	.L126
.L137:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$40, %eax
	jmp	.L126
.L138:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$44, %eax
	jmp	.L126
.L139:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$48, %eax
	jmp	.L126
.L140:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$52, %eax
	jmp	.L126
.L141:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$56, %eax
	jmp	.L126
.L142:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$60, %eax
	jmp	.L126
.L143:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$64, %eax
	jmp	.L126
.L144:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$68, %eax
	jmp	.L126
.L145:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$72, %eax
	jmp	.L126
.L146:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$76, %eax
	jmp	.L126
.L148:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$84, %eax
	jmp	.L126
.L147:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$80, %eax
	jmp	.L126
.L125:
	orl	$-1, %eax
	jmp	.L126
.L188:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$244, %eax
	jmp	.L126
.L187:
	xorl	%eax, %eax
	tzcntl	%ecx, %eax
	sarl	$3, %eax
	addl	$240, %eax
	jmp	.L126
.L186:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$236, %eax
	jmp	.L126
.L185:
	xorl	%eax, %eax
	tzcntl	%ecx, %eax
	sarl	$3, %eax
	addl	$232, %eax
	jmp	.L126
.L184:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$228, %eax
	jmp	.L126
.L183:
	xorl	%eax, %eax
	tzcntl	%ecx, %eax
	sarl	$3, %eax
	addl	$224, %eax
	jmp	.L126
.L182:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$220, %eax
	jmp	.L126
.L181:
	xorl	%eax, %eax
	tzcntl	%ecx, %eax
	sarl	$3, %eax
	addl	$216, %eax
	jmp	.L126
.L180:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$212, %eax
	jmp	.L126
.L179:
	xorl	%eax, %eax
	tzcntl	%ecx, %eax
	sarl	$3, %eax
	addl	$208, %eax
	jmp	.L126
.L178:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$204, %eax
	jmp	.L126
.L177:
	xorl	%eax, %eax
	tzcntl	%ecx, %eax
	sarl	$3, %eax
	addl	$200, %eax
	jmp	.L126
.L176:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$196, %eax
	jmp	.L126
.L175:
	xorl	%eax, %eax
	tzcntl	%ecx, %eax
	sarl	$3, %eax
	addl	$192, %eax
	jmp	.L126
.L174:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$188, %eax
	jmp	.L126
.L173:
	xorl	%eax, %eax
	tzcntl	%ecx, %eax
	sarl	$3, %eax
	addl	$184, %eax
	jmp	.L126
.L172:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$180, %eax
	jmp	.L126
.L171:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$176, %eax
	jmp	.L126
.L170:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$172, %eax
	jmp	.L126
.L169:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$168, %eax
	jmp	.L126
.L168:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$164, %eax
	jmp	.L126
.L167:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$160, %eax
	jmp	.L126
.L166:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$156, %eax
	jmp	.L126
.L165:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$152, %eax
	jmp	.L126
.L164:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$148, %eax
	jmp	.L126
.L163:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$144, %eax
	jmp	.L126
.L162:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$140, %eax
	jmp	.L126
.L161:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$136, %eax
	jmp	.L126
.L160:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$132, %eax
	jmp	.L126
.L159:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	subl	$-128, %eax
	jmp	.L126
.L158:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$124, %eax
	jmp	.L126
.L157:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$120, %eax
	jmp	.L126
.L156:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$116, %eax
	jmp	.L126
.L155:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$112, %eax
	jmp	.L126
.L154:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$108, %eax
	jmp	.L126
.L153:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$104, %eax
	jmp	.L126
.L152:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$100, %eax
	jmp	.L126
.L151:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$96, %eax
	jmp	.L126
.L150:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$92, %eax
	jmp	.L126
.L149:
	tzcntl	%eax, %eax
	sarl	$3, %eax
	addl	$88, %eax
	jmp	.L126
	.cfi_endproc
.LFE7869:
	.size	_Z20linear_search_avx_urPKmim, .-_Z20linear_search_avx_urPKmim
	.p2align 4
	.globl	_Z7set_ymmv
	.type	_Z7set_ymmv, @function
_Z7set_ymmv:
.LFB7870:
	.cfi_startproc
	endbr64
	pushq	%rbx
	.cfi_def_cfa_offset 16
	.cfi_offset 3, -16
	xorl	%eax, %eax
	movl	$-1, %ebx
#APP
# 185 "src/search.cc" 1
	vpbroadcastd %ebx,%ymm0
	vpmovmskb %ymm0,%eax
# 0 "" 2
#NO_APP
	popq	%rbx
	.cfi_def_cfa_offset 8
	ret
	.cfi_endproc
.LFE7870:
	.size	_Z7set_ymmv, .-_Z7set_ymmv
	.section	.rodata.str1.1
.LC1:
	.string	"data1:%x\n"
	.text
	.p2align 4
	.globl	_Z7clr_ymmv
	.type	_Z7clr_ymmv, @function
_Z7clr_ymmv:
.LFB7871:
	.cfi_startproc
	endbr64
	pushq	%rbx
	.cfi_def_cfa_offset 16
	.cfi_offset 3, -16
	xorl	%eax, %eax
	movl	%eax, %ebx
	leaq	.LC1(%rip), %rsi
#APP
# 196 "src/search.cc" 1
	vpbroadcastd %ebx,%ymm0
	vpmovmskb %ymm0,%eax
# 0 "" 2
#NO_APP
	movl	$1, %edi
	movl	%eax, %edx
	popq	%rbx
	.cfi_def_cfa_offset 8
	xorl	%eax, %eax
	jmp	__printf_chk@PLT
	.cfi_endproc
.LFE7871:
	.size	_Z7clr_ymmv, .-_Z7clr_ymmv
	.ident	"GCC: (Ubuntu 9.4.0-1ubuntu1~20.04.1) 9.4.0"
	.section	.note.GNU-stack,"",@progbits
	.section	.note.gnu.property,"a"
	.align 8
	.long	 1f - 0f
	.long	 4f - 1f
	.long	 5
0:
	.string	 "GNU"
1:
	.align 8
	.long	 0xc0000002
	.long	 3f - 2f
2:
	.long	 0x3
3:
	.align 8
4:
