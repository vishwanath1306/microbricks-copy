/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#include "work.h"

namespace hindsightgrpc {
    double matrix_multiply(const MatrixConfig& config) {
        double M1[config.m_][config.n_], M2[config.n_][config.k_], M3[config.m_][config.k_];

        for (int i = 0; i < config.m_; ++i)
            for (int j = 0; j < config.k_; ++j)
                for (int k = 0; k < config.n_; ++k) {
                    M3[i][j] += M1[i][k] * M2[k][j];
                }

        double total = 0;
        for (int i = 0; i < config.m_; ++i)
            for (int j = 0; j < config.k_; ++j)
                total += M3[i][j];
        
        return total;
    }
}