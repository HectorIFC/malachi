/* xoshiro128** 1.1 and splitmix64, verbatim from https://prng.di.unimi.it/ (Blackman and Vigna). */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
static uint64_t x; /* splitmix64 state */
uint64_t splitmix64_next(void) {
	uint64_t z = (x += 0x9e3779b97f4a7c15);
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
	return z ^ (z >> 31);
}
static inline uint32_t rotl(const uint32_t x, int k) { return (x << k) | (x >> (32 - k)); }
static uint32_t s[4];
uint32_t next(void) {
	const uint32_t result = rotl(s[1] * 5, 7) * 9;
	const uint32_t t = s[1] << 9;
	s[2] ^= s[0]; s[3] ^= s[1]; s[1] ^= s[2]; s[0] ^= s[3];
	s[2] ^= t;
	s[3] = rotl(s[3], 11);
	return result;
}
int main(int argc, char **argv) {
	for (int a = 1; a < argc; a++) {
		x = strtoull(argv[a], NULL, 0);
		uint64_t p = splitmix64_next(), q = splitmix64_next();
		s[0] = (uint32_t)p; s[1] = (uint32_t)(p >> 32); s[2] = (uint32_t)q; s[3] = (uint32_t)(q >> 32);
		printf("%s:", argv[a]);
		for (int i = 0; i < 16; i++) printf(" %u", next());
		printf("\n");
	}
	return 0;
}
