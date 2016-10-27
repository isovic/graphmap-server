/*
 * semaphore.h
 *
 *  Created on: Oct 25, 2016
 *      Author: Robert Vaser (99%) and Ivan Sovic (1%).
 */

#pragma once

#include <stdint.h>
#include <memory>
#include <vector>
#include <queue>
#include <mutex>
#include <thread>
#include <future>
#include <atomic>
#include <condition_variable>
#include <assert.h>

class Semaphore;

std::unique_ptr<Semaphore> createSemaphore(uint32_t value);

class Semaphore {
public:

    ~Semaphore() = default;

    uint32_t value() const {
        return value_;
    }

    void wait();
    void post();

    friend std::unique_ptr<Semaphore> createSemaphore(uint32_t value);

private:

    Semaphore(uint32_t value);
    Semaphore(const Semaphore&) = delete;
    const Semaphore& operator=(const Semaphore&) = delete;

    std::mutex mutex_;
    std::condition_variable condition_;
    uint32_t value_;
};

