#pragma once
#include "Envelope.hpp"

struct Sink {
    void* ctx{};

    void (*submit)(void*, Envelope&&) = nullptr;

    void (*flush)(void*, int) = nullptr;
};