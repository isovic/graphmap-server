/*
 * semaphore.cc
 *
 *  Created on: Oct 25, 2016
 *      Author: isovic
 */

#include "semaphore.h"

std::unique_ptr<Semaphore> createSemaphore(uint32_t value) {
    return std::unique_ptr<Semaphore>(new Semaphore(value));
}

Semaphore::Semaphore(uint32_t value)
        : value_(value) {
}

void Semaphore::post() {

    std::unique_lock<std::mutex> lock(mutex_);
    ++value_;
    condition_.notify_one();
}

void Semaphore::wait() {

    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock, [&](){ return value_; });
    --value_;
}
