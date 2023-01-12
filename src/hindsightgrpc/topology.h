/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#pragma once
#ifndef SRC_HINDSIGHTGRPC_TOPOLOGY_H_
#define SRC_HINDSIGHTGRPC_TOPOLOGY_H_

#include <json.hpp>

#include <iostream>
#include <vector>
#include <map>

#include "work.h"

using json = nlohmann::json;

namespace hindsightgrpc {
  struct AddressInfo {
    public:
      AddressInfo(std::string name, std::string port, std::string deploy_addr, std::string hostname, std::string agent_port) 
      : name(name), deploy_addr(deploy_addr) {
        hostnames.push_back(hostname);
        ports.push_back(port);
        agent_ports.push_back(agent_port);
        connection_addresses.push_back(hostname + ":" + port);
        breadcrumbs.push_back(hostname + ":" + agent_port);
        num_instances = 1;
      }
      AddressInfo(std::string name, std::string deploy_addr, std::vector<json> instances)
      : name(name), deploy_addr(deploy_addr) {
        for (auto i : instances) {
          std::string hostname = i["hostname"];
          std::string port = i["port"];
          std::string agent_port = i["agent_port"];
          hostnames.push_back(hostname);
          ports.push_back(port);
          agent_ports.push_back(agent_port);
          connection_addresses.push_back(hostname + ":" + port);
          breadcrumbs.push_back(hostname + ":" + agent_port);
        }
        num_instances = instances.size();
      }
      AddressInfo() {}
      // std::string hostname;
      // std::string port;
      std::string deploy_addr;
      std::string name;
      // std::string agent_port;
      // std::string connection_address;
      // std::string breadcrumb;
      std::vector<std::string> hostnames;
      std::vector<std::string> ports;
      std::vector<std::string> agent_ports;
      std::vector<std::string> connection_addresses;
      std::vector<std::string> breadcrumbs;
      // std::string get_connection_address() { return hostname + ":" + port; }
      // std::string get_breadcrumb() { return hostname + ":" + agent_port; }
      int num_instances;
  };

  /* A potential outgoing call to an api in a different service*/
  class Outcall {
    public:
      Outcall(std::string service_name, std::string api_name, int probability,
              std::vector<std::string> connection_addresses,
              std::vector<std::string> breadcrumbs)
          : service_name(service_name),
            api_name(api_name),
            probability(probability) {
        unique_name = service_name + ":" + api_name;
        int num_instances = connection_addresses.size();
        assert(num_instances == breadcrumbs.size());
        if (num_instances == 1) {
          server_addr = connection_addresses[0];
          breadcrumb = breadcrumbs[0];
        } else {
          for (int i = 0; i < num_instances; i++) {
            subcalls.push_back(Outcall(service_name, api_name, probability,
                                      connection_addresses[i], breadcrumbs[i]));
          }
        }
      }
      Outcall(std::string service_name, std::string api_name, int probability, std::string server_addr, std::string breadcrumb) 
      : service_name(service_name), api_name(api_name), probability(probability), server_addr(server_addr), breadcrumb(breadcrumb) {
        unique_name = service_name + ":" + api_name;
      }
      friend std::ostream& operator<<(std::ostream& os, const Outcall& outcall) {
        os << outcall.service_name << "," << outcall.api_name << "," << outcall.probability;
        return os;
      }

      // std::string get_unique_name() { return unique_ }
      // std::string &get_breadcrumb() { return breadcrumb; }
      // std::string &get_service_name() { return service_name; }
      // std::string &get_api_name() { return api_name; }
      // int get_probability() { return probability; }
      // std::string &get_address() { return server_addr; }

      std::string service_name;
      std::string api_name;
      std::string unique_name;
      int probability;
      std::string server_addr;
      std::string breadcrumb;
      // revealed when picking a instance for the service
      std::vector<Outcall> subcalls;
  };

  /* An API provided by the service */
  class API {
    public:
      API(std::string name, double exec, std::vector<Outcall> children) : name(name), exec(exec), children(children) {}
      API() {}
      friend std::ostream& operator<<(std::ostream& os, const API& api) {
        os << api.name << ": " << api.exec << "\n";
        for (auto child : api.children) {
            os << "\t\t" << child << "\n";
        }
        return os;  
      }
    
      std::vector<Outcall> children;
      
      std::string name;
      double exec;
  };

  /* A service config*/
  class ServiceConfig {
    public:
      ServiceConfig(std::string name, std::map<std::string, API> apis) : name(name), apis(apis) {}

      friend std::ostream& operator<<(std::ostream& os, const ServiceConfig& config) {
        os << config.name << "\n";
        for (auto api : config.apis) {
          os << "\t" << api.second << "\n";
        }
        return os;
      }

      std::string& Name() {return name;}

      API& get_api(std::string api_name) { return apis[api_name]; }

      const std::map<std::string, API>& get_apis() { return apis; }

      MatrixConfig& get_matrix_config(std::string api_name) { return api_matrix_configs[api_name]; }

      void print_matrix_configs() {
        for (auto it = api_matrix_configs.begin(); it != api_matrix_configs.end(); ++it) {
          std::cout << "Config for api " << it->first << " is: (" << it->second.m_ << "," << it->second.n_ << "," << it->second.k_ << ")\n";
        }
      }

      void generate_matrix_configs();

    private:
      std::string name;
      std::map<std::string, API> apis;
      std::map<std::string, MatrixConfig> api_matrix_configs;
  };

  json parse_config(std::string fname);
  ServiceConfig get_service_config(json global_config, std::string service_name, std::map<std::string, AddressInfo>& addresses);
  std::map<std::string, AddressInfo> get_address_map(json global_config);

} // namespace hindsightgrpc

#endif  // SRC_HINDSIGHTGRPC_TOPOLOGY_H_