/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <limits>
#include <cmath>

#include "topology.h"

#include <json.hpp>

using json = nlohmann::json;

namespace hindsightgrpc {
    json parse_config(std::string filename) {
        std::ifstream fin(filename);
        json j;
        fin >> j;
        return j;
    }

    ServiceConfig get_service_config(
        json global_config,
        std::string service_name,
        std::map<std::string, AddressInfo>& addresses) {
        std::map<std::string, API> apis;
        bool found = false;
        for (auto it : global_config["services"]) {
            if (it["name"] == service_name) {
            for (auto ait : it["apis"]) {
                std::vector<Outcall> children;
                for (auto chit : ait["children"]) {
                    std::string service_name(chit["service"]);
                    Outcall child =
                        Outcall(chit["service"], chit["api"], chit["probability"],
                                addresses[service_name].connection_addresses,
                                addresses[service_name].breadcrumbs);
                    children.push_back(child);
                }
                API api = API(ait["name"], ait["exec"], children);
                apis[ait["name"]] = api;
            }
            // We have found the service!
            found = true;
            break;
            }
        }
        if (!found) {
            service_name = "";
        }
        return ServiceConfig(service_name, apis);
    }

    std::map<std::string, AddressInfo> get_address_map(json global_config) {
        std::map<std::string, AddressInfo> addresses;

        for (auto it : global_config["addresses"]) {
            if (it.count("instances") == 0) {
                addresses[it["name"]] = AddressInfo(it["name"],
                                                it["port"],
                                                it["deploy_addr"],
                                                it["hostname"],
                                                it["agent_port"]);
            } else {
                addresses[it["name"]] = AddressInfo(it["name"], it["deploy_addr"], it["instances"]);
            }
        }

        return addresses;
    }

    void ServiceConfig::generate_matrix_configs() {
        // TODO: Possibly convert this into an option.
        std::string fname("../config/matrix_benchmarks.csv");
        std::fstream fin(fname, std::ios::in);
        std::map<double, MatrixConfig> loaded_configs;
        if (fin.is_open()) {
            std::vector<std::string> row;
            std::string line, word;
            int count = 0;
            while(std::getline(fin, line)) {
                row.clear();

                std::stringstream str(line);
                while(std::getline(str, word, ',')) {
                    row.push_back(word);
                }

                if (count == 0) {
                    // Ignore header row
                    count += 1;
                    continue;
                }

                int m = std::stoi(row[0]);
                int n = std::stoi(row[1]);
                int k = std::stoi(row[2]);
                double val = std::stod(row[3]);
                loaded_configs[val] = MatrixConfig(m, n, k);
                count += 1;
            }

            for (auto it = apis.begin(); it != apis.end(); ++it) {
                MatrixConfig config;
                double target = (double) (it->second.exec);
                double min_val = std::numeric_limits<double>::max();
                for (auto val_it = loaded_configs.begin(); val_it != loaded_configs.end(); ++val_it) {
                    double abs_value = std::abs(target - val_it->first);
                    if (abs_value < min_val) {
                        min_val = abs_value;
                        config.m_ = val_it->second.m_;
                        config.n_ = val_it->second.n_;
                        config.k_ = val_it->second.k_;
                    }
                }
                api_matrix_configs[it->first] = config;
            }
        }
        // TODO: Handle the case when the file was not opened.
    }

}  // namespace hindsightgrpc
