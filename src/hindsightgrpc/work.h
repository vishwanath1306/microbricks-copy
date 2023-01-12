/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#pragma once
#ifndef SRC_HINDSIGHTGRPC_WORK_H_
#define SRC_HINDSIGHTGRPC_WORK_H_

#include "iostream"

namespace hindsightgrpc {
  /* Configuration for the matrix multiplication task */
  struct MatrixConfig {
    public:
    MatrixConfig(int m, int n, int k) : m_(m), n_(n), k_(k) {}
    MatrixConfig() : m_(50), n_(50), k_(50){}
    int m_, n_, k_;

    friend std::ostream& operator<<(std::ostream& os, const MatrixConfig& mc) {
      os << "[" << mc.m_ << ", " << mc.n_ << ", " << mc.k_ << "]";
      return os;
    }
  };

  double matrix_multiply(const MatrixConfig& config);
} // namespace hindsightgrpc

#endif  // SRC_HINDSIGHTGRPC_TOPOLOGY_H_