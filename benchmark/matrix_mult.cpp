#include <iostream>
#include <chrono>
#include <vector>

void run_matrix_mult(int m_, int n_, int k_) {
	double M1[m_][n_], M2[n_][k_], M3[m_][k_];

	for (int i = 0; i < m_; ++i)
		for (int j = 0; j < k_; ++j)
			for (int k = 0; k < n_; k++) {
				M3[i][j] += M1[i][k] * M2[k][j];
			}
}

int main() {
	std::vector<int> config{25, 50, 100, 250, 500};
	constexpr int NUM_ITERATIONS = 50;
	std::cout << "m,n,k,time(ms)\n";

	for (int i = 0; i < config.size(); ++i) {
		for (int j = 0; j < config.size(); ++j) {
			for (int k = 0; k < config.size(); ++k) {
				int m_ = config[i];
				int n_ = config[j];
				int k_ = config[k];
				double sum_times = 0.0;
				for (int i = 0; i < NUM_ITERATIONS; ++i) {
					auto begin = std::chrono::high_resolution_clock::now();

					run_matrix_mult(m_, n_, k_);

					auto end = std::chrono::high_resolution_clock::now();
					auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
					sum_times += elapsed.count() * 1e-6;
				}
				double avg_time = sum_times / NUM_ITERATIONS;
				std::cout << m_ << "," << n_ << "," << k_ << "," << avg_time << "\n" ;
			}
		}
	}

	return 0;	
}
