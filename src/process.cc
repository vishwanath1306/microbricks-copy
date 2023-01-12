/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#include "tracing/hindsight_extensions.h"
#include <map>
#include <set>
#include <vector>
#include <string>
#include <iomanip>
#include <argp.h>
#include <fstream>
#include <sstream>

extern "C" {
  #include "tracestate.h"
}

const char *program_version = "hindsight-process 1.0";
const char *program_bug_address = "<cld-science@mpi-sws.org>";
static char doc[] = "Process data received by Hindsight's backend into traces and calculate trace completion.  Takes as argument the collector data file";
static char args_doc[] = "FILENAME";

static struct argp_option options[] = {
  {"debug",  'd', 0,  0,  "Print debug information.  Spammy." },
  {"warn",  'w', 0,  0,  "Print information about malformed traces." },
  { 0 }
};

struct arguments {
  bool debug;
  bool warn;
  std::string inputfile;
  std::string outputfile;
};

bool debug = false;
bool warn = false;

static error_t parse_opt (int key, char *arg, struct argp_state *state) {
  struct arguments *arguments = (struct arguments*) state->input;

  switch (key)
    {
    case 'd':
      arguments->debug = true;
      break;
    case 'w':
      arguments->warn = true;
      break;
    case ARGP_KEY_ARG:
      if (state->arg_num >= 1)
        /* Too many arguments. */
        argp_usage (state);

      arguments->inputfile = std::string(arg);
      break;

    case ARGP_KEY_END:
      if (state->arg_num < 1)
        /* Not enough arguments. */
        argp_usage (state);
      break;

    default:
      return ARGP_ERR_UNKNOWN;
    }
  return 0;
}

static struct argp argp = { options, parse_opt, args_doc, doc };

int readLengthPrefixed(std::fstream &f, char* &dst) {
  char szbuf[4];
  if (!f.read(szbuf, 4)) {
    return 0;
  }
  int size = *((int*) &szbuf);
  if (size < 0 || size > (1024 * 1024 * 100)) {
    std::cout << "Likely invalid size " << size << " read" << std::endl;
    return 0;
  }
  dst = (char*) malloc(size);
  if (!f.read(dst, size)) {
    free(dst);
    dst = nullptr;
    return 0;
  }
  return size;
}

enum class TraceStatus {
  kValid = 0,
  kIgnore,
  kMissingPrevBuffer,
  kMultipleNextBuffers,
  kPrematureEndOfSlice,
  kDuplicateBufferID,
  kEmptyTrace,
  kMissingChildCalls,
  kMissingAttributeValue,
  kMissingSpanStart,
  kMissingSpanEnd,
  kUnexpectedBreadcrumb
};

static std::map<TraceStatus,std::string> const statuses = {
  {TraceStatus::kValid, "Valid"},
  {TraceStatus::kIgnore, "Trace with the 'Ignore' attribute set to true."},
  {TraceStatus::kMissingPrevBuffer, "A buffer references another buffer that doesn't exist"},
  {TraceStatus::kMultipleNextBuffers, "Multiple buffers have the same buffer marked as prev (this is a special case we don't currently handle)"},
  {TraceStatus::kPrematureEndOfSlice, "Buffers ended with a partial fragment of trace data"},
  {TraceStatus::kDuplicateBufferID, "Multiple buffers have the same buffer ID (this is a special case we don't currently handle)"},
  {TraceStatus::kEmptyTrace, "The trace somehow contained no RPC calls"},
  {TraceStatus::kMissingChildCalls, "The number of RPCs executed did not match the number of child calls made."},
  {TraceStatus::kMissingAttributeValue, "The span attributes weren't formatted correctly."},
  {TraceStatus::kMissingSpanStart, "Span was ended but not started."},
  {TraceStatus::kMissingSpanEnd, "Span was started but not ended."},
  {TraceStatus::kUnexpectedBreadcrumb, "A breadcrumb was found but not in an Exec or Childcall span"}
};

inline std::string traceStatusDescription(TraceStatus status) {
  auto it = statuses.find(status);
  if (it != statuses.end()) {
    return it->second;
  } else {
    return "";
  }
}


class RawHindsightBuffer {
public:
  std::string agent;
  TraceHeader* header;
  char* buf;
  size_t size;
  RawHindsightBuffer* next;
  RawHindsightBuffer* prev;

  RawHindsightBuffer(std::string &agent, char* buf, size_t size) 
    : agent(agent), buf(buf), size(size), next(nullptr), prev(nullptr), header((TraceHeader*) buf) {
  }

  ~RawHindsightBuffer() {
    if (buf != nullptr) {
      free(buf);
    }
  }

  std::string str() {
    std::stringstream s;
    s << "Buffer[";
    s << "Agent=" << agent;
    s << ", TraceID=" << header->trace_id;
    s << ", N=" << header->buffer_number;
    s << "]";
    return s.str();
  }
};

RawHindsightBuffer* readNextBuffer(std::fstream &f) {
  char* agentbuf;
  int agentsize = readLengthPrefixed(f, agentbuf);
  if (agentsize == 0) {
    return nullptr;
  }
  std::string agent = std::string(agentbuf, agentsize);
  free(agentbuf);

  char* buf;
  int bufsize = readLengthPrefixed(f, buf);
  if (bufsize == 0) {
    return nullptr;
  }

  if (bufsize < 32) {
    if (warn) {
      std::cout << "Warning: invalid buffer encountered, with size " << bufsize << std::endl;
    }
    free(buf);
    return nullptr;
  }

  return new RawHindsightBuffer(agent, buf, bufsize);
}

void readBuffers(std::string filename, std::vector<RawHindsightBuffer*> &buffers) {
  std::fstream f(filename, std::ios_base::in);

  f.seekg(0, std::ios::end);
  size_t length = f.tellg();
  f.seekg(0, std::ios::beg);
  if (debug) {
    std::cout << filename << " has length " << length << std::endl;
  }

  while (f.peek() != EOF) {
    RawHindsightBuffer* b = readNextBuffer(f);
    if (b != nullptr) {
      buffers.push_back(b);
    }
  }
}

/* A single entry of a trace.  Corresponds to an Event in hindsight_extensions,
but additionally includes the agent where the data was reported, and a slice ID */
class TraceEntry {
public:
  Event header;
  char* payload;

  std::string stringvalue() {
    return std::string(payload, header.size);
  }

  int64_t intvalue() {
    if (header.size == 4) {
      return (int64_t) *((int*) payload);
    } else if (header.size == 8) {
      return *((int64_t*) payload);
    } else {
      return -5;
    }
  }
};

/* Combines multiple sequential buffers into a single contiguous one, and drops the trace headers from each buffer */
class CombinedBuffer {
public:
  std::string agent;
  size_t size;
  char* buf;

  CombinedBuffer(const std::string &agent, std::vector<RawHindsightBuffer*> &buffers) : agent(agent), size(0), buf(nullptr) {
    for (auto &raw : buffers) {
      size += raw->size - sizeof(TraceHeader);
    }    
    buf = (char*) malloc(size);
    size_t offset = 0;
    for (auto &raw : buffers) {
      size_t copy_size = raw->size - sizeof(TraceHeader);
      std::memcpy(buf + offset, raw->buf + sizeof(TraceHeader), copy_size);
      offset += copy_size;
    }
  }
  ~CombinedBuffer() {
    if (buf != nullptr) {
      free(buf);
    }
  }

  TraceStatus extractEntries(std::vector<TraceEntry> &dst) {
    std::vector<TraceEntry> entries;
    size_t offset = 0;
    while (offset < size) {
      if (offset + sizeof(Event) > size) {
        return TraceStatus::kPrematureEndOfSlice;
      }
      TraceEntry entry;
      entry.header = *((Event*) (this->buf + offset));
      offset += sizeof(Event);
      entry.payload = this->buf + offset;
      if (offset + entry.header.size > this->size) {
        return TraceStatus::kPrematureEndOfSlice;
      }
      offset += entry.header.size;
      dst.push_back(entry);
    }
    return TraceStatus::kValid;
  }

};

/*
Processes all of the buffers received from a given agent.
*/
TraceStatus groupAndConcatenate(
      const std::string &agent, 
      std::vector<RawHindsightBuffer*> &buffers,
      std::vector<CombinedBuffer*> &combined
    ) {
    if (debug) {
      std::cout << "Agent " << agent << " has " << buffers.size() << " buffers" << std::endl;
    }

    std::map<int, RawHindsightBuffer*> lookup;
    for (auto &buf : buffers) {
      int id = buf->header->buffer_id;
      if (lookup.find(id) != lookup.end()) {
        // TODO? In principle we can deal with duplicate buffer IDs, but it's rare enough
        //   that for now we can ignore it.
        if (warn) {
          std::cout << "Duplicate " << id << " found" << std::endl;
        }
        return TraceStatus::kDuplicateBufferID;
      } else {
        lookup[id] = buf;
        if (debug) {
          std::cout << " " << id << " length:" << buf->size << std::endl;
        }
      }
    }

    std::vector<RawHindsightBuffer*> roots;

    for (auto &buf : buffers) {
      int id = buf->header->buffer_id;
      int prev_id = buf->header->prev_buffer_id;

      if (id == prev_id) {
        roots.push_back(buf);
      } else {
        if (lookup.find(prev_id) == lookup.end()) {
          if (warn) {
            std::cout << "Buffer " << id << " missing parent " << prev_id << std::endl;
          }
          return TraceStatus::kMissingPrevBuffer;
        } else {
          auto &prev = lookup[prev_id];
          if (prev->next != nullptr) {
            if (warn) {
              std::cout << "Multiple buffers think " << prev_id << " is its parent" << std::endl;
            }
            return TraceStatus::kMultipleNextBuffers;
          } else {
            buf->prev = prev;
            prev->next = buf;
          }
        }
      }
    }

    if (debug) {
      std::cout << roots.size() << " roots" << std::endl;
    }

    for (RawHindsightBuffer* buf : roots) {
      std::vector<RawHindsightBuffer*> bufs_to_concat;
      while (buf != nullptr) {
        bufs_to_concat.push_back(buf);
        buf = buf->next;
      }
      combined.push_back(new CombinedBuffer(agent, bufs_to_concat));
    }

    if (debug) {
      std::cout << "Combined " << buffers.size() << " buffers into " << combined.size() << std::endl;
      for (auto &cmb : combined) {
        std::cout << cmb->size << std::endl;
      }
    }

    return TraceStatus::kValid;
}

/* An attribute is recorded as two TraceEntries, one for the key and one for the value.
This method finds the values for all attributes of a specified key. */
TraceStatus findAttributeEntries(std::vector<TraceEntry> &entries, std::string key, std::vector<TraceEntry> &value_entries) {
  TraceStatus status = TraceStatus::kValid;

  std::vector<int> value_indices;
  for (int i = 0; i < entries.size(); i++) {
    auto &entry = entries[i];
    if (entry.header.type != EventType::kAttributeKey) continue;
    if (entry.stringvalue() != key) continue;
    value_indices.push_back(i+1);
  }

  for (auto &i : value_indices) {
    if (i >= entries.size()) {
      status = TraceStatus::kMissingAttributeValue;
      continue;
    }

    auto &entry = entries[i];
    if (entry.header.type != EventType::kAttributeValue) {
      status = TraceStatus::kMissingAttributeValue;
      continue;
    }

    value_entries.push_back(entry);
  }

  return status;
}

/* For a specified key, find all values as strings */
TraceStatus findStringAttributes(std::vector<TraceEntry> &entries, std::string key, std::vector<std::string> &values) {
  std::vector<TraceEntry> value_entries;
  TraceStatus status = findAttributeEntries(entries, key, value_entries);

  for (auto &entry : value_entries) {
    values.push_back(entry.stringvalue());
  }
  return status;
}

/* For a specified key, find all values as ints */
TraceStatus findIntAttributes(std::vector<TraceEntry> &entries, std::string key, std::vector<int64_t> &values) {
  std::vector<TraceEntry> value_entries;
  TraceStatus status = findAttributeEntries(entries, key, value_entries);

  for (auto &entry : value_entries) {
    values.push_back(entry.intvalue());
  }
  return status;
}


TraceStatus makeTrace(
    std::vector<RawHindsightBuffer*> &buffers,
    std::vector<CombinedBuffer*> &combined,
    std::set<int64_t> &intervals,
    std::set<int64_t> &triggers
  ) {
  TraceStatus status = TraceStatus::kValid;

  /* Each agent can potentially send us multiple buffers:
     (1) if a single call exceeded the size of one buffer, it will spill into a second buffer
     (2) if the request made multiple calls to the same service
  */
  std::map<std::string, std::vector<RawHindsightBuffer*>> buffers_by_agent;
  for (auto &buffer : buffers) {
    buffers_by_agent[buffer->agent].push_back(buffer);
  }

  /* For case (1) above, we concatenate the individual buffers into a single buffer
    It's possible that case (1) occurs but the spillover buffer doesn't exist (was dropped)
    in which case it's an incomplete trace */
  for (auto &p : buffers_by_agent) {
    TraceStatus st = groupAndConcatenate(p.first, p.second, combined);
    status = (status == TraceStatus::kValid) ? st : status;
  }

  /* Before returning invalid status, we want to try to find the group this
  trace belongs to, and the triggers that fired it */
  for (auto &cmb : combined) {
    std::vector<TraceEntry> entries;
    cmb->extractEntries(entries);

    std::vector<int64_t> interval_attrs;
    findIntAttributes(entries, "Interval", interval_attrs);
    for (auto &interval : interval_attrs) {
      intervals.insert(interval);
    }

    std::vector<int64_t> trigger_attrs;
    findIntAttributes(entries, "Trigger", trigger_attrs);
    for (auto &trigger : trigger_attrs) {
      triggers.insert(trigger);
    }
  }
  if (status != TraceStatus::kValid) {
    return status;
  }

  /* In hindsight-grpc, the senders and receivers of RPC calls will always record breadcrumbs
  to each other.
  So we can validate a trace by ensuring that all such breadcrumb pairs exist on both
  sender side and receiver side */
  std::map<std::pair<std::string, std::string>, int> sender_side_calls;
  std::map<std::pair<std::string, std::string>, int> receiver_side_calls;

  for (auto &cmb : combined) {
    /* TODO(jcmace): this could be a lot faster using iterators but currently
    speed of development > speed of execution. */
    std::vector<TraceEntry> entries;
    status = cmb->extractEntries(entries);
    if (status != TraceStatus::kValid) {
      return status;
    }

    if (debug) {
      std::cout << "Extracted " << entries.size() << " entries from " << cmb->agent << std::endl;
    }

    /* Need to know the mapping from span id to span name */
    std::map<int, std::string> span_names;
    for (int i = 0; i < entries.size(); i++) {
      auto &entry = entries[i];
      if (entry.header.type != EventType::kSpanName) continue;
      span_names[entry.header.span_id] = entry.stringvalue();
    }

    std::vector<TraceEntry> breadcrumb_entries;
    status = findAttributeEntries(entries, "Breadcrumb", breadcrumb_entries);
    if (status != TraceStatus::kValid) {
      return status;
    }

    /* All calls are expected to show up on both the sender and receiver side */
    for (auto &entry : breadcrumb_entries) {
      std::string breadcrumb = entry.stringvalue();
      
      auto &span_name = span_names[entry.header.span_id];
      if (span_name == "HindsightGRPC/Exec") {
        receiver_side_calls[{breadcrumb, cmb->agent}]++;
      } else if (span_name == "HindsightGRPC/ChildCall/Prepare") {
        sender_side_calls[{cmb->agent, breadcrumb}]++;
      } else {
        return TraceStatus::kUnexpectedBreadcrumb;
      }
    }

    /* All spans should have a start and end event */
    std::map<uint64_t, int> spans;
    for (auto &entry : entries) {
      if (entry.header.type == EventType::kSpanStart) {
        spans[entry.header.span_id]++;
      } else if (entry.header.type == EventType::kSpanEnd) {
        spans[entry.header.span_id]--;
      }
    }
    if (spans.size() == 0) {
      return TraceStatus::kEmptyTrace;
    }
    for (auto &p : spans) {
      if (p.second < 0) {
        return TraceStatus::kMissingSpanStart;
      } else if (p.second > 0) {
        return TraceStatus::kMissingSpanEnd;
      }
    }
  }

  /* All calls are expected to show up on the sender and receiver side */
  if (receiver_side_calls.size() != sender_side_calls.size()) {
    return TraceStatus::kMissingChildCalls;
  }
  for (auto &p : sender_side_calls) {
    auto it = receiver_side_calls.find(p.first);
    if (it == receiver_side_calls.end() || p.second != it->second) {
      if (warn) {
        std::cout << "Call missing: " << p.first.first << " -> " << p.first.second << std::endl;
      }
      return TraceStatus::kMissingChildCalls;
    }
  }

  return TraceStatus::kValid;
}

void process(struct arguments args) {
  std::vector<RawHindsightBuffer*> buffers;

  /* Load the data from file into memory */
  readBuffers(args.inputfile, buffers);
  std::cout << "Read " << buffers.size() << " buffers from " << args.inputfile << std::endl;

  /* A file typically contains buffers from multiple traces,
  so group the buffers by trace_id */
  std::map<uint64_t, std::vector<RawHindsightBuffer*>> grouped;
  for (auto &buf : buffers) {
    grouped[buf->header->trace_id].push_back(buf);
  }
  std::cout << grouped.size() << " traces total" << std::endl;

  /* Now we just process the traces */
  std::map<std::pair<int64_t, int64_t>, std::map<int, int>> outcomes;
  for (auto &p : grouped) {
    std::vector<CombinedBuffer*> combined;
    std::set<int64_t> intervals;
    std::set<int64_t> triggers;


    TraceStatus status = makeTrace(p.second, combined, intervals, triggers);
    if (debug || (warn && status != TraceStatus::kValid)) {
      std::cout << "Trace " << p.first << " status is " <<  ((int) status) << ": " << traceStatusDescription(status) << std::endl;
    }

    // Use -9 for "none"
    if (intervals.size() == 0) {
      intervals.insert(-9);
    }
    if (triggers.size() == 0) {
      triggers.insert(-9);
    }

    // Use -10 for totals
    intervals.insert(-10);
    triggers.insert(-10);

    for (auto &interval : intervals) {
      for (auto &trigger : triggers) {
        outcomes[{interval, trigger}][(int) status]++;
      }
    }

    for (auto &cmb : combined) {
      delete cmb;
    }
  }

  int64_t min_interval = INT64_MAX;
  for (auto &o : outcomes) {
    int64_t interval = o.first.first;
    if (interval >= 0 && interval < min_interval) {
      min_interval = interval;
    }    
  }

  std::cout << std::setw(4) << "I" << std::setw(8) << "Trigger" << std::setw(7) << "Status" << std::setw(8) << "Count" << std::setw(7) << "Pct" << " Description" << std::endl;
  for (auto &o : outcomes) {
    int64_t interval = o.first.first;
    int64_t trigger = o.first.second;

    std::string interval_name;
    if (interval == -10) interval_name = "All";
    else if (interval == -9) interval_name = "x";
    else interval_name = std::to_string(interval - min_interval);

    std::string trigger_name = std::to_string(trigger);
    if (trigger == -10) trigger_name = "All";
    if (trigger == -9) trigger_name = "x";

    int total = 0;
    for (auto &p : o.second) {
      total += p.second;
    }

    bool first = true;
    for (auto &p : o.second) {
      TraceStatus status = (TraceStatus) p.first;
      int count = p.second;
      float pct = (100 * p.second) / (float) total;

      std::cout << std::setw(4) << interval_name;
      std::cout << std::setw(8) << trigger_name;
      std::cout << std::setw(7) << (int) status;
      std::cout << std::setw(8) << count;
      std::cout << std::setw(7) << std::fixed << std::setprecision(2) << pct;
      std::cout << " " << traceStatusDescription(status) << std::endl;

      first = false;
    }
  }

  for (auto &raw : buffers) {
    delete raw;
  }
}

int main(int argc, char** argv) {

  struct arguments arguments;
  arguments.debug = false;
  arguments.warn = false;

  /* Parse the arguments */
  argp_parse (&argp, argc, argv, 0, 0, &arguments);

  debug = arguments.debug;
  warn = arguments.warn || arguments.debug;

  std::cout << "Processing " << arguments.inputfile << std::endl;
  process(arguments);

  return 0;
}
