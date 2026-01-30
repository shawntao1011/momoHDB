#pragma once
#include <iosfwd>

struct k0;
using K = k0*;

void print_k(K x, std::ostream& os, int depth = 0);
