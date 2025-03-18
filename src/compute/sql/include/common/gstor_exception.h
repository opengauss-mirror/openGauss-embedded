//===----------------------------------------------------------------------===//
// Copyright 2018-2023 Stichting DuckDB Foundation
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice (including the next paragraph)
// shall be included in all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <exception>
#include <string>

enum class ExceptionType : uint8_t {
    INVALID = 0,             // invalid type
    OUT_OF_RANGE,            // value out of range error
    CONVERSION,              // conversion/casting error
    UNKNOWN_TYPE,            // unknown type
    DECIMAL,                 // decimal related
    MISMATCH_TYPE,           // type mismatch
    DIVIDE_BY_ZERO,          // divide by 0
    OBJECT_SIZE,             // object size exceeded
    INVALID_TYPE,            // incompatible for operation
    SERIALIZATION,           // serialization
    TRANSACTION,             // transaction management
    NOT_IMPLEMENTED,         // method not implemented
    EXPRESSION,              // expression parsing
    CATALOG,                 // catalog related
    PARSER,                  // parser related
    PLANNER,                 // planner related
    SCHEDULER,               // scheduler related
    EXECUTOR,                // executor related
    CONSTRAINT,              // constraint related
    INDEX,                   // index related
    STAT,                    // stat related
    CONNECTION,              // connection related
    SYNTAX,                  // syntax related
    SETTINGS,                // settings related
    BINDER,                  // binder related
    NETWORK,                 // network related
    OPTIMIZER,               // optimizer related
    NULL_POINTER,            // nullptr exception
    IO,                      // IO exception
    INTERRUPT,               // interrupt
    FATAL,                   // Fatal exceptions are non-recoverable, and render the entire DB in an unusable state
    INTERNAL,                // Internal exceptions indicate something went wrong internally (i.e. bug in the code base)
    INVALID_INPUT,           // Input or arguments error
    OUT_OF_MEMORY,           // out of memory
    PERMISSION,              // insufficient permissions
    PARAMETER_NOT_RESOLVED,  // parameter types could not be resolved
    PARAMETER_NOT_ALLOWED,   // parameter types not allowed
    ACCOUNT_AUTH_FAILED,     // Incorrect user or password
};