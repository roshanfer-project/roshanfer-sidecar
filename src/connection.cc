#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <connection.h>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <liburing.h>
#include <nghttp2/nghttp2.h>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "buffer.h"
#include "connection_enums.h"
#include "glog/logging.h"
#include "ingress.h"
#include "picohttpparser.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "stats.h"
#include "config.h"
#include <memory>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <netinet/tcp.h>

std::string type_to_str(ConnectionType type) {
    if (type == ConnectionType::INGRESS) {
        return "INGRESS";
    } else if (type == ConnectionType::EGRESS) {
        return "EGRESS";
    } else {
        LOG(FATAL) << "Unknown connection type";
    }
};

std::string direction_to_str(ConnectionDirection direction) {
    if (direction == ConnectionDirection::UPSTREAM) {
        return "UPSTREAM";
    } else if (direction == ConnectionDirection::DOWNSTREAM) {
        return "DOWNSTREAM";
    } else {
        LOG(FATAL) << "Unknown connection direction";
    }
};

std::string frame_type_to_str(uint8_t type) {
    switch (type) {
        case NGHTTP2_DATA:
            return "DATA";
        case NGHTTP2_HEADERS:
            return "HEADERS";
        case NGHTTP2_PRIORITY:
            return "PRIORITY";
        case NGHTTP2_RST_STREAM:
            return "RST_STREAM";
        case NGHTTP2_SETTINGS:
            return "SETTINGS";
        case NGHTTP2_PUSH_PROMISE:
            return "PUSH_PROMISE";
        case NGHTTP2_PING:
            return "PING";
        case NGHTTP2_GOAWAY:
            return "GOAWAY";
        case NGHTTP2_WINDOW_UPDATE:
            return "WINDOW_UPDATE";
        case NGHTTP2_CONTINUATION:
            return "CONTINUATION";
        default:
            return "UNKNOWN";
    }
};

int error_callback(nghttp2_session */*session*/,
                    int lib_error_code,
                    const char *msg,
                    size_t len,
                    void *user_data) {
    
    CallbackData* data = reinterpret_cast<CallbackData*>(user_data);

    LOG(FATAL) << "nghttp2 error (" << lib_error_code << ") on fd=" <<  data->fd << " : " 
              << std::string(msg, len);
    return 0;
}

int invalid_frame_callback(nghttp2_session */*session*/,
                            const nghttp2_frame *frame,
                            int /*lib_error_code*/,
                            void */*user_data*/) {
    LOG(WARNING) << "Invalid frame received: " << frame_type_to_str(frame->hd.type);
    return 0;
}

// This callback is used to detect EOS flag for a stream, which marks the end of a request/response
// to put in a queue for for forwarding/routing
int frame_recv_callback(nghttp2_session* /*session*/,
                        const nghttp2_frame* frame,
                        void* user_data) {
    
    CallbackData* data = reinterpret_cast<CallbackData*>(user_data);

    if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
        VLOG(1) << "EOS flag detected on fd: " << data->fd << " stream id: " << frame->hd.stream_id;

        if (frame->hd.type == NGHTTP2_DATA) {
            // we have a request
            VLOG(1) << "RPC request received on fd: " << data->fd << " stream id: " << frame->hd.stream_id;
            data->queue->enqueue(data->type, data->direction, data->fd, frame->hd.stream_id);

            // record receive time for the RPC
            data->mapper->get_ds_rpc(data->type, frame->hd.stream_id, data->fd)->req_rcv_time
             = std::chrono::steady_clock::now();
        }
        else if (frame->hd.type == NGHTTP2_HEADERS) {
            // we have a response
            VLOG(1) << "RPC response received on fd: " << data->fd << " stream id: " << frame->hd.stream_id;
            data->queue->enqueue(data->type, data->direction, data->fd, frame->hd.stream_id);
            auto rpc = dynamic_cast<gRPCMessage*>(data->mapper->get_us_rpc(data->type, frame->hd.stream_id, data->fd));
            rpc->res_rcv_time = std::chrono::steady_clock::now();
            if (rpc->get_data_map().at(1)->offset == 0) {
                // This is an error response
                VLOG(1) << "RPC error detected on fd: " << data->fd  << " stream id: " << frame->hd.stream_id;
                rpc->set_error(true);
            }
        }
    } else if (frame->hd.type == NGHTTP2_SETTINGS) {
        VLOG(1) << "SETTINGS frame received on fd: " << data->fd << " stream id: " << frame->hd.stream_id;
        if (*data->status == ConnectionStatus::DOWN) {
            *data->status = ConnectionStatus::UP;
            VLOG(1) << "Change status to UP, fd: " << data->fd << " stream id: " << frame->hd.stream_id;
        }
    } else {
        VLOG(1) << "Frame type " << frame_type_to_str(frame->hd.type) << " received on fd: " << data->fd << " stream id: " << frame->hd.stream_id;
    }
    return 0;
}

// This callback is used to receive data chunks
int on_data_chunk_recv_callback(nghttp2_session* /*session*/,
                                uint8_t /*flags*/,
                                int32_t stream_id,
                                const uint8_t* data,
                                size_t len,
                                void* user_data) {

    CallbackData* callback_data = reinterpret_cast<CallbackData*>(user_data);
    
    if (callback_data->direction == ConnectionDirection::DOWNSTREAM) {
        try {
            callback_data->mapper->get_ds_rpc(callback_data->type, stream_id, callback_data->fd)->add_data(
                data, len, true);
        } catch (const std::out_of_range& e) {
            LOG(FATAL) << "No RPC object found for stream_id: " << stream_id;
        }
    } else {
        try {
            callback_data->mapper->get_us_rpc(callback_data->type, stream_id, callback_data->fd)->add_data(
                data, len, false);
        } catch (const std::out_of_range& e) {
            LOG(FATAL) << "No RPC object found for stream_id: " << stream_id;
        }
    }

    VLOG(1) << "on_data_chunk_recv_callback fd: " << callback_data->fd << " stream id: " << stream_id << " of length: " << len;
    return 0;
}

// This callback is used to receive headers
int on_header_callback(nghttp2_session* /*session*/,
                        const nghttp2_frame* frame,
                        const uint8_t* name, size_t namelen,
                        const uint8_t* value, size_t valuelen,
                        uint8_t /*flags*/,
                        void* user_data) {

    CallbackData* data = reinterpret_cast<CallbackData*>(user_data);

    if (frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        if (data->direction != ConnectionDirection::DOWNSTREAM) {
            LOG(FATAL) << "Invalid direction for request frame";
        }

        // a request header
        VLOG(1) << "Request header received on fd: " << data->fd << " stream id: " << frame->hd.stream_id;
        data->mapper->get_ds_rpc(data->type, frame->hd.stream_id, data->fd)->add_header_field(
            name, namelen, value, valuelen, true, false);
    } else if (frame->headers.cat == NGHTTP2_HCAT_HEADERS) {
        if (data->direction != ConnectionDirection::UPSTREAM) {
            LOG(FATAL) << "Invalid direction for response frame";
        }

        VLOG(1) << "tailer header received on fd: " << data->fd << " stream id:" << frame->hd.stream_id;
        
        data->mapper->get_us_rpc(data->type, frame->hd.stream_id, data->fd)->add_header_field(
            name, namelen, value, valuelen, false, true);
    } else if (frame->headers.cat == NGHTTP2_HCAT_RESPONSE) {
        if (data->direction != ConnectionDirection::UPSTREAM) {
            LOG(FATAL) << "Invalid direction for response frame";
        }

        VLOG(1) << "Response header received on fd: " << data->fd << " stream id: " << frame->hd.stream_id;
        data->mapper->get_us_rpc(data->type, frame->hd.stream_id, data->fd)->add_header_field(
            name, namelen, value, valuelen, false, false);
    }

    VLOG(1) << "Header received: "
    << std::string(reinterpret_cast<const char*>(name), namelen)
    << " : " << std::string(reinterpret_cast<const char*>(value), valuelen);
    return 0;
}

// This callback is used to create the objects to hold request/response
int on_begin_headers_callback(nghttp2_session* /*session*/,
                            const nghttp2_frame* frame,
                            void* user_data) {
    
    CallbackData* data = reinterpret_cast<CallbackData*>(user_data);
    if (frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        if (data->direction != ConnectionDirection::DOWNSTREAM) {
            LOG(FATAL) << "Invalid direction for request frame";
        }

        // we have a new request
        VLOG(1) << "New request on stream " << frame->hd.stream_id;
        data->mapper->allocate_rpc(data->type, frame->hd.stream_id, data->fd, HTTP::HTTP2);
    }

    VLOG(1) << "Begin headers on stream " << frame->hd.stream_id << " fd: " << data->fd;
    return 0;
}

/* int on_stream_close_callback(nghttp2_session *session,
                            int32_t stream_id,
                            uint32_t error_code,
                            void *user_data) {

    CallbackData* data = reinterpret_cast<CallbackData*>(user_data);
    
    auto& rpc = data->mapper->get_ds_rpc(data->type, stream_id);

    // calculate the duration
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - rpc->get_rcv_time());

    LOG(INFO) << "Latency: " << duration.count() << " us" 
            << " type: " << type_to_str(data->type);
        
    // remove the RPC object
    //data->mapper->remove_rpc(data->type, stream_id);
    VLOG(1) << "Stream " << stream_id << " closed on fd: " << data->fd;
    return 0;
} */

int on_frame_send_callback(nghttp2_session */*session*/,
                            const nghttp2_frame *frame,
                            void *user_data) {
    
    CallbackData* data = reinterpret_cast<CallbackData*>(user_data);

    VLOG(1) << "Frame type " << frame_type_to_str(frame->hd.type) << " sent on fd: " << data->fd << " stream id: " << frame->hd.stream_id;
    return 0;
}

/*
    Note that in this callback we have upstream fd and stream id
*/
ssize_t data_read_callback_request(nghttp2_session*,
                        int32_t stream_id,
                        uint8_t* buf,
                        size_t length,
                        uint32_t* data_flags,
                        nghttp2_data_source* source,
                        void* user_data) {
    
    DataReadStruct* info = reinterpret_cast<DataReadStruct*>(source->ptr);
    CallbackData* callback_data = reinterpret_cast<CallbackData*>(user_data);

    VLOG(1) << "data_read_callback_request, data len: " << info->offset
            << " stream id: " << stream_id
            << " fd: " << callback_data->fd;

    // If the output buffer is too small, copy what fits.
    ssize_t res_len;
    if (length < info->offset - info->read_offset) {
        LOG(FATAL) << "FIXME: Like data_read_callback_response + use copy_n";
        std::memcpy(buf, info->data.data()+info->read_offset, length);
        info->read_offset += length;
        res_len = (ssize_t)length;
    } else {
        std::copy_n(info->data.begin() + info->read_offset, info->offset - info->read_offset, buf);
        info->read_offset += info->offset;
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        res_len = (ssize_t)info->offset;
    }

    // record the time
    callback_data->mapper->get_us_rpc(callback_data->type, stream_id, callback_data->fd)->req_for_time
     = std::chrono::steady_clock::now();

    return res_len;
};

ssize_t data_read_callback_response(nghttp2_session* session,
                            int32_t stream_id,
                            uint8_t* buf,
                            size_t length,
                            uint32_t* data_flags,
                            nghttp2_data_source* source,
                            void* user_data) {

    DataReadStruct* info = reinterpret_cast<DataReadStruct*>(source->ptr);
    CallbackData* callback_data = reinterpret_cast<CallbackData*>(user_data);

    VLOG(1) << "data_read_callback_response, data len: " << info->offset
            << " stream id: " << stream_id
            << " fd: " << callback_data->fd;
    
    VLOG(2) << "Before read. offset: " << info->offset
                << " read_offset: " << info->read_offset
                << " length: " << length;
    ssize_t res_len;
    if (info->offset == 0) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        res_len = 0;
        LOG(WARNING) << "No data to send";
    } else {
        // If the output buffer is too small, copy what fits.
        
        size_t remain = info->offset - info->read_offset;
        size_t to_copy = std::min(remain, length);
        std::memcpy(buf, info->data.data() + info->read_offset, to_copy);
        info->read_offset += to_copy;
        res_len = (ssize_t)to_copy;

        VLOG(2) << "After read. offset: " << info->offset
                << " read_offset: " << info->read_offset
                << " length: " << length;

        // only signal EOF on the last chunk
        if (info->read_offset == info->offset) {
            *data_flags |= NGHTTP2_DATA_FLAG_EOF;
            *data_flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;

            auto rpc = dynamic_cast<gRPCMessage*>(callback_data->mapper->get_ds_rpc(callback_data->type, stream_id, callback_data->fd));

            nghttp2_nv* nva_trailers = new nghttp2_nv[(size_t)rpc->get_res_trailer_count()];
            auto& res_trailers = rpc->get_res_trailers();
            for (size_t i = 0; i < rpc->get_res_trailer_count(); i++) {
                nva_trailers[i].name = res_trailers.at(i)->name.data();
                nva_trailers[i].value = res_trailers.at(i)->value.data();
                nva_trailers[i].namelen = res_trailers.at(i)->name_len;
                nva_trailers[i].valuelen = res_trailers.at(i)->value_len;
                nva_trailers[i].flags = NGHTTP2_NV_FLAG_NONE;
            }

            if (nghttp2_submit_trailer(session, rpc->get_ds_stream_id(), nva_trailers,
                    rpc->get_res_trailer_count()) != 0) {
                LOG(FATAL) << "Failed to submit HTTP/2 response trailers";
            }
            delete [] nva_trailers;

            // report latency
            if (config.report_latency) {
                report_latency(*rpc, callback_data->type, callback_data->hist);
            }
            
            // remove the RPC message from memory
            auto orig_rpc = dynamic_cast<RPCMessage*>(rpc);
            callback_data->mapper->remove_rpc(callback_data->type, orig_rpc);
        }
    }
    
    return res_len;
};

ConnectionPool::ConnectionPool(ConnectionType conn_type) 
    : connections(std::unordered_map<int, std::unique_ptr<HTTPConnection>>()),
      type(conn_type) {};

std::unique_ptr<HTTPConnection>& ConnectionPool::add_connection(const std::string& host,
     int port, RPCMapper* mapper, RPCQueue* queue, HTTP http, struct hdr_histogram* hist) {
    std::unique_ptr<HTTPConnection> c;
    if (http == HTTP::HTTP1) {
        c = std::make_unique<HTTP1Connection>(host, port, mapper, queue, hist);
    } else {
        c = std::make_unique<HTTP2Connection>(host, port, type, queue, mapper, hist);
    }
    int fd = c->get_fd();
    connections.emplace(fd, std::move(c));
    return connections.at(fd);
};

bool ConnectionPool::has_connection(int fd) {
    return connections.find(fd) != connections.end();
};

// This should return any "available" connections.
// HTTP/1.1 connections doen't allow multiplexing.
std::unique_ptr<HTTPConnection>& ConnectionPool::get_any_connection() {
    if (connections.empty()) {
        throw NoConnectionException("Pool is empty");
    }

    // return first available connection
    for (auto& conn : connections) {
        if (conn.second->available()) {
            return conn.second;
        }
    }
    throw NoConnectionException("No available connections in the pool");
};


///// HTTPConnection implementation

HTTPConnection::HTTPConnection(int conn_fd, ConnectionType conn_type, struct hdr_histogram* hist_struct) 
    :   fd(conn_fd),
        addr({}),
        status(ConnectionStatus::UP),
        host(""),
        port(0),
        hist(hist_struct),
        type(conn_type),
        direction(ConnectionDirection::DOWNSTREAM) 
{
    
    // set non-blocking
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        LOG(FATAL) << "Failed to get flags for fd: " << fd;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        LOG(FATAL) << "Failed to set non-blocking for fd: " << fd;
    }

    int flag = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) == -1) {
        LOG(FATAL) << "Failed to set TCP_NODELAY";
    }
};

HTTPConnection::HTTPConnection(std::string conn_host, uint16_t conn_port, ConnectionType conn_type, struct hdr_histogram* hist_struct) 
    :   fd(0),
        addr({}),
        status(ConnectionStatus::DOWN),
        host(conn_host),
        port(conn_port),
        hist(hist_struct),
        type(conn_type),
        direction(ConnectionDirection::UPSTREAM)
{

    fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }

    VLOG(1) << "Created fd: " << fd << " for host: " << host << " port: " << port;

    // Perform DNS resolution using getaddrinfo
    struct addrinfo hints, *result;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;       // IPv4
    hints.ai_socktype = SOCK_STREAM;

    int rv = getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &result);
    if (rv != 0) {
        close(fd);
        LOG(FATAL) << "DNS resolution failed for " << host << ": " << gai_strerror(rv);
    }

    // Copy resolved address to our member variable addr
    struct sockaddr_in* addr_in = reinterpret_cast<struct sockaddr_in*>(result->ai_addr);
    addr = *addr_in;

    // Log the resolved IP address
    char ip_str[INET_ADDRSTRLEN];
    if (inet_ntop(AF_INET, &(addr_in->sin_addr), ip_str, sizeof(ip_str)) == nullptr) {
        LOG(FATAL) << "Failed to convert resolved address to string";
    } else {
        VLOG(1) << "Resolved address for " << host << ": " << ip_str;
    }

    freeaddrinfo(result);

    int flag = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) == -1) {
        LOG(FATAL) << "Failed to set TCP_NODELAY";
    }
}

std::string HTTPConnection::type_to_str() {
    if (type == ConnectionType::INGRESS) {
        return "INGRESS";
    } else if (type == ConnectionType::EGRESS) {
        return "EGRESS";
    } else {
        LOG(FATAL) << "Unknown connection type";
    }
};

std::string HTTPConnection::direction_to_str() {
    if (direction == ConnectionDirection::UPSTREAM) {
        return "UPSTREAM";
    } else if (direction == ConnectionDirection::DOWNSTREAM) {
        return "DOWNSTREAM";
    } else {
        LOG(FATAL) << "Unknown connection direction";
    }
};

sockaddr* HTTPConnection::get_addr() {
    return reinterpret_cast<sockaddr*>(&addr);
}


//////// HTTP1Connection implementation

HTTP1Connection::HTTP1Connection(std::string conn_host, uint16_t conn_port, RPCMapper* rpc_mapper, RPCQueue* rpc_queue,
                                 struct hdr_histogram* hist_struct)
    : HTTPConnection(conn_host, conn_port, ConnectionType::INGRESS, hist_struct),
      buf(std::array<char, HTTP1Connection_BUF_SIZE>()),
      buf_len(0),
      prev_buf_len(0),
      hdr_complete(false),
      content_length(-1),
      hdr_size(0),
      idle(true),
      mapper(rpc_mapper),
      queue(rpc_queue),
      last_id(0),
      rpc_message(nullptr) {


    VLOG(1) << "Created HTTP1Connection for host: " << host << " port: " << port 
            << ", fd: " << fd 
            << ", type: " << type_to_str() 
            << ", direction: " << direction_to_str();
}

HTTP1Connection::HTTP1Connection(int conn_fd, RPCMapper* rpc_mapper, RPCQueue* rpc_queue, struct hdr_histogram* hist_struct)
    : HTTPConnection(conn_fd, ConnectionType::INGRESS, hist_struct),
      buf(std::array<char, HTTP1Connection_BUF_SIZE>()),
      buf_len(0),
      prev_buf_len(0),
      hdr_complete(false),
      content_length(-1),
      hdr_size(0),
      idle(true),
      mapper(rpc_mapper),
      queue(rpc_queue),
      last_id(0),
      rpc_message(nullptr) {

    VLOG(1) << "Created HTTP1Connection for fd: " << fd 
            << ", type: " << type_to_str() 
            << ", direction: " << direction_to_str();
}

HTTP1Connection::~HTTP1Connection() {
    VLOG(1) << "HTTP1Connection deconstructor on fd: " << fd;
    close(fd);
    //delete[] buf;
}

static inline bool header_body_allowed(int code)
{
    if ((100 <= code && code < 200) || code == 204 || code == 304) return false;
    return true;
}


void HTTP1Connection::http_read(Buffer* buffer, Ingress& ingress) {
    // copy at most HTTP1Connection_BUF_SIZE-buf_len bytes from Buffer to buf
    if (buffer->get_filled() == 0) {
        VLOG(1) << "No data to read on fd: " << fd;
        return;
    }

    if (buf_len + buffer->get_filled() >= HTTP1Connection_BUF_SIZE) {
        LOG(FATAL) << "Buffer overflow, buf_len: " << buf_len 
                    << ", buffer filled: " << buffer->get_filled();
    }
    std::copy_n(buffer->data.begin(), buffer->get_filled(), buf.begin() + buf_len);
    prev_buf_len = buf_len;
    buf_len += buffer->get_filled();

    size_t num_headers = HTTP1Connection_MAX_HEADERS;
    struct phr_header headers[HTTP1Connection_MAX_HEADERS];

    if (direction == ConnectionDirection::DOWNSTREAM) {
        // we are reading a request
        VLOG(1) << "Reading HTTP/1.1 request on fd: " << fd;

        const char *method, *path;
        size_t method_len, path_len;
        int minor;
        
        hdr_size = phr_parse_request(
            buf.data(), buf_len,
            &method, &method_len,
            &path,   &path_len,
            &minor,
            headers, &num_headers,
            prev_buf_len
        ); 

        if (hdr_size == -2) {
            VLOG(1) << "Not enough data to parse HTTP/1.1 request on fd: " << fd;
            return;
        } else if (hdr_size == -1) {
            LOG(FATAL) << "Failed to parse HTTP/1.1 request on fd: " << fd
                << ", buf: " << std::string(buf.data(), buf_len);
        }

        // we have a complete headers
        VLOG(1) << "HTTP/1.1 request parsed on fd: " << fd 
                << ", method: " << std::string(method, method_len)
                << ", path: " << std::string(path, path_len)
                << ", lenght: " << hdr_size;
        
        if (VLOG_IS_ON(2)) {
            for (size_t i = 0; i < num_headers; i++) {
            VLOG(2) << "Header: " << std::string(headers[i].name, headers[i].name_len)
                << ": " << std::string(headers[i].value, headers[i].value_len);
            }
        }

        for (size_t i = 0; i < num_headers; i++) {
            if (headers[i].name_len >= 14) {
                if (strncmp(headers[i].name, "Content-Length", 14) == 0) {
                    content_length = (int)strtoul(headers[i].value, NULL, 10);
                }
            }
        }
            
        if (content_length >= 0) {
            LOG(FATAL) << "Content-Length header is not supported in HTTP/1.1 requests, fd: " << fd
                << ", content_length: " << content_length;
        }

        if (buf_len > (size_t)hdr_size) {
            LOG(FATAL) << "We have more data than headers size in request parsing, buf_len: " << buf_len
                << ", hdr_size: " << hdr_size;
        }

        // create a new RPCMessage object
        last_id++;
        mapper->allocate_rpc(type, last_id, fd, HTTP::HTTP1);
        HTTPMessage* rpc = dynamic_cast<HTTPMessage*>(mapper->get_ds_rpc(type, last_id, fd));
        rpc->req_rcv_time = std::chrono::steady_clock::now();
        
        // fill the RPCMessage
        for (size_t i = 0; i < num_headers; i++) {
            rpc->add_header_field(
                reinterpret_cast<const uint8_t*>(headers[i].name), headers[i].name_len,
                reinterpret_cast<const uint8_t*>(headers[i].value), headers[i].value_len,
                true, false);
        }
        rpc->set_method(method, method_len);
        rpc->set_service(path, path_len);
        rpc->set_path(path, path_len);
        rpc->set_minor(minor);

        // Send RPC to Ingress
        ingress.enqueue(rpc);

        // update idle
        if (idle == false) {
            VLOG(1) << "Reading a request on non-idle connection, fd: " << fd
                << ", type: " << type_to_str() 
                << ", direction: " << direction_to_str()
                << ", buf: " << std::string(buf.data(), buf_len);
        }
        idle = false;
        
        // reset state
        buf_len = 0;
        prev_buf_len = 0;
        hdr_complete = false;
        content_length = -1;
        hdr_size = 0;

        VLOG(2) << "Internal state after parsing HTTP/1.1 request on fd: " << fd
                << ", buf_len: " << buf_len
                << ", prev_buf_len: " << prev_buf_len
                << ", header_size: " << hdr_size;
        
    }
    else if (direction == ConnectionDirection::UPSTREAM) {
        // we are reading a response
        
        VLOG(1) << "Reading HTTP/1.1 response on fd: " << fd;

        if (!hdr_complete) {

            int minor, parse_status;
            const char *msg;
            size_t msg_len;

            hdr_size = phr_parse_response(buf.data(), buf_len,
                        &minor, &parse_status,
                        &msg, &msg_len,
                        headers, &num_headers,
                        prev_buf_len);
            
            if (hdr_size == -2) {
                VLOG(1) << "Not enough data to parse HTTP/1.1 request on fd: " << fd;
                return;
            } else if (hdr_size == -1) {
                LOG(FATAL) << "Failed to parse HTTP/1.1 request on fd: " << fd;
            }

            VLOG(1) << "HTTP/1.1 response parsed on fd: " << fd 
                    << ", status: " << parse_status
                    << ", msg: " << std::string(msg, msg_len)
                    << ", minor: " << minor
                    << ", lenght: " << hdr_size;

            if (VLOG_IS_ON(2)) {
                for (size_t i = 0; i < num_headers; i++) {
                VLOG(2) << "Header: " << std::string(headers[i].name, headers[i].name_len)
                    << ": " << std::string(headers[i].value, headers[i].value_len);
                }
            }

            if (header_body_allowed(parse_status)) {
                for (size_t i = 0; i < num_headers; i++) {
                    if (headers[i].name_len == 14) {
                        if (strncasecmp(headers[i].name, "Content-Length", 14) == 0) {
                            auto [_, ec] = std::from_chars(headers[i].value,
                                 headers[i].value + headers[i].value_len, content_length);
                            if (ec != std::errc() && content_length != -1) {
                                LOG(FATAL) << "Failed to parse Content-Length header, fd: " << fd
                                            << ", value: " << std::string(headers[i].value, headers[i].value);
                                
                            }
                        }
                    }

                    if (headers[i].name_len == 17 && headers[i].value_len >= 7) {
                        if (strncasecmp(headers[i].name, "Transfer-Encoding", 17) == 0 &&
                            strncasecmp(headers[i].value, "chunked", 7) == 0) {
                                LOG(FATAL) << "Chunked transfer encoding is not supported in HTTP/1.1 responses, fd: " << fd
                                            << ", value: " << std::string(headers[i].value, headers[i].value_len);
                            }
                    }
                }
            }

            // we have a complete headers
            hdr_complete = true;

            // fill the RPCMessage
            HTTPMessage* rpc = dynamic_cast<HTTPMessage*>(mapper->get_us_rpc(type, last_id, fd));
            for (size_t i = 0; i < num_headers; i++) {
                rpc->add_header_field(
                    reinterpret_cast<const uint8_t*>(headers[i].name), headers[i].name_len,
                    reinterpret_cast<const uint8_t*>(headers[i].value), headers[i].value_len,
                    false, false);
            }
            rpc->set_status(parse_status);
            rpc->set_msg(msg, msg_len);
            rpc->set_minor(minor);
        }

        if ((int)buf_len-hdr_size < content_length) {
            VLOG(1) << "Not enough data to parse HTTP/1.1 response on fd: " << fd
                    << ", buf_len: " << buf_len
                    << ", hdr_size: " << hdr_size
                    << ", content_length: " << content_length;
            return;
        }

        // we have a full body
        if (content_length > 0) {
            const char* body = buf.data() + hdr_size;

            // add the data
            HTTPMessage* rpc = dynamic_cast<HTTPMessage*>(mapper->get_us_rpc(type, last_id, fd));
            rpc->res_rcv_time = std::chrono::steady_clock::now();
            rpc->add_data(reinterpret_cast<const uint8_t*>(body), (size_t)content_length, false);
        } else {
            content_length = 0;
        }

        // push the RPC to RPCQueue
        queue->enqueue(type, direction, fd, last_id);
        
        if ((int)buf_len > hdr_size + content_length) {
            LOG(FATAL) << "We have more data than headers size + body size in response parsing, buf_len: " << buf_len
                << ", hdr_size: " << hdr_size
                << ", content_length: " << content_length;
        }

        // update idle
        if (idle == true) {
            VLOG(1) << "Reading a response from idle connection, fd: " << fd
                << ", type: " << type_to_str() 
                << ", direction: " << direction_to_str()
                << ", buf: " << std::string(buf.data(), buf_len);
        }
        idle = true;

        // reset state
        buf_len = 0;
        prev_buf_len = 0;
        hdr_complete = false;
        content_length = -1;
        hdr_size = 0;

        VLOG(2) << "Internal state after parsing HTTP/1.1 response on fd: " << fd
                << ", buf_len: " << buf_len
                << ", prev_buf_len: " << prev_buf_len
                << ", body_size: " << content_length
                << ", header_size: " << hdr_size;
    }
}

bool HTTP1Connection::want_write() {
    return rpc_message!= nullptr;
}

/* bool HTTP1Connection::want_read() {
    LOG(FATAL) << "HTTP/1.1 connection does not support want_read";
} */

void static inline snprintf_ret_check(int ret, size_t size) {
    if (ret < 0 || (size_t)ret >= size) {
        LOG(FATAL) << "snprintf failed or buffer overflow, ret: " << ret << ", size: " << size;
    }
}

int HTTP1Connection::http_write(Buffer* buffer) {
    if (direction == ConnectionDirection::UPSTREAM) {
        // we are serializing a request

        int written = 0;
        HTTPMessage* rpc = get_rpc_message();
        int ret;
        size_t size;

        size = buffer->get_size()-buffer->get_filled();
        ret = snprintf(buffer->data.data()+written, size,
                            "%.*s %.*s HTTP/1.%d\r\n",
                            (int)rpc->get_method().length(), rpc->get_method().c_str(),
                            (int)rpc->get_path().length(),   rpc->get_path().c_str(),
                            rpc->get_minor());
        
        snprintf_ret_check(ret, size);
        written += ret;
        buffer->set_filled((size_t)ret + buffer->get_filled());

        auto& headers = rpc->get_req_headers();
        for (size_t i = 0; i < rpc->get_req_header_count(); i++) {
            size = buffer->get_size()-buffer->get_filled();
            ret = snprintf(buffer->data.data()+written,
                        size,
                            "%.*s: %.*s\r\n",
                            (int)headers.at(i)->name_len,   headers.at(i)->name.data(),
                            (int)headers.at(i)->value_len,  headers.at(i)->value.data());
            snprintf_ret_check(ret, size);
            written += ret;
            buffer->set_filled((size_t)ret + buffer->get_filled());
        }

        size = buffer->get_size()-buffer->get_filled();
        ret = snprintf(buffer->data.data()+written, size, "\r\n");
        snprintf_ret_check(ret, size);
        written += ret;
        buffer->set_filled((size_t)ret + buffer->get_filled());

        if (buffer->get_filled() > buffer->get_size()) {
            LOG(FATAL) << "Buffer overflow";
        }

        VLOG(1) << "Write " << written << " bytes to HTTP/1.1 request on fd: " << fd;

        if (VLOG_IS_ON(2)) {
            // log the entire request
            std::string request(buffer->data.data(), buffer->get_filled());
            VLOG(2) << "HTTP/1.1 request on fd: " << fd << "\n" << request;
        }

        rpc->req_for_time = std::chrono::steady_clock::now();

        // update idle
        if (idle == false) {
            VLOG(1) << "Writing a request to non-idle connection, fd: " << fd
                << ", type: " << type_to_str() 
                << ", direction: " << direction_to_str()
                << ", buf: " << std::string(buf.data(), buf_len);
        }
        idle = false;

        return written;
    }
    else {
        // we are serializing a response
       
        int written = 0;
        auto rpc = get_rpc_message();
        int ret;
        size_t size;

        if (rpc->is_error()) {
            // return a 503 error response
            size = buffer->get_size()-buffer->get_filled();
            ret = snprintf(buffer->data.data()+written, 
                            size,
                            "HTTP/1.%d 503 Service Unavailable\r\n"
                            "Content-Type: text/plain\r\n"
                            "Content-Length: 0\r\n"
                            "\r\n", rpc->get_minor());
            snprintf_ret_check(ret, size);
            written += ret;
            buffer->set_filled((size_t)ret + buffer->get_filled());
            if (buffer->get_filled() > buffer->get_size()) {
                LOG(FATAL) << "Buffer overflow";
            }
            VLOG(1) << "Write " << written << " bytes to HTTP/1.1 error response on fd: " << fd;
        } else {
            size = buffer->get_size()-buffer->get_filled();
            ret = snprintf(buffer->data.data()+written, size,
                            "HTTP/1.%d %d %.*s\r\n",
                            rpc->get_minor(), rpc->get_status(),
                            (int)rpc->get_msg().length(), rpc->get_msg().c_str());
            snprintf_ret_check(ret, size);
            written += ret;
            buffer->set_filled((size_t)ret + buffer->get_filled());
        
            auto& headers = rpc->get_res_headers();
            for (size_t i = 0; i < rpc->get_res_header_count(); i++) {
                size = buffer->get_size()-buffer->get_filled();
                ret = snprintf(buffer->data.data()+written, size,
                                "%.*s: %.*s\r\n",
                                (int)headers.at(i)->name_len,   headers.at(i)->name.data(),
                                (int)headers.at(i)->value_len,  headers.at(i)->value.data());
                snprintf_ret_check(ret, size);
                written += ret;
                buffer->set_filled((size_t)ret + buffer->get_filled());
            }
            
            size = buffer->get_size()-buffer->get_filled();
            ret = snprintf(buffer->data.data()+written, size, "\r\n");
            snprintf_ret_check(ret, size);
            written += ret;
            buffer->set_filled((size_t)ret + buffer->get_filled());

            auto body = rpc->get_res_data();
            if (body.offset >= buffer->get_size()-buffer->get_filled()) {
                LOG(FATAL) << "Buffer overflow, body size: " << body.offset
                    << ", buffer size: " << buffer->get_size()
                    << ", filled: " << buffer->get_filled();
            }
            std::copy_n(body.data.begin(), body.offset, buffer->data.begin() + (long)buffer->get_filled());
            written += (int)body.offset;

            // update the buffer size
            buffer->set_filled(body.offset + buffer->get_filled());

            if (buffer->get_filled() > buffer->get_size()) {
                LOG(FATAL) << "Buffer overflow";
            }
            VLOG(1) << "Write " << written << " bytes to HTTP/1.1 response on fd: " << fd;
        }

        // update idle
        if (idle == true) {
            LOG(FATAL) << "Writing a response to idle connection, fd: " << fd
                << ", type: " << type_to_str() 
                << ", direction: " << direction_to_str()
                << ", buf: " << std::string(buf.data(), buf_len); 
        }
        idle = true;

        if (config.report_latency) {
            report_latency(*rpc, type, hist);
        }

        RPCMessage* orig_rpc = dynamic_cast<RPCMessage*>(rpc);
        mapper->remove_rpc(type, orig_rpc);

        return written;
    }
    
}

void HTTP1Connection::submit_settings() {
    VLOG(1) << "submit_settings is a no-op for HTTP/1.1 connections";
}

int32_t HTTP1Connection::submit_request(RPCMessage& rpc) {
    set_rpc_message(dynamic_cast<HTTPMessage*>(&rpc));
    last_id++;
    return last_id;
}

void HTTP1Connection::submit_response(RPCMessage& rpc) {
    set_rpc_message(dynamic_cast<HTTPMessage*>(&rpc));
}

void HTTP1Connection::submit_error_response(RPCMessage& rpc) {
    submit_response(rpc);
}

bool HTTP1Connection::available() {
    return idle;
}

void HTTP1Connection::set_rpc_message(HTTPMessage* msg) {
    if (rpc_message != nullptr) {
        LOG(FATAL) << "RPC message is already set";
    }
    rpc_message = msg;
}

HTTPMessage* HTTP1Connection::get_rpc_message() {
    if (rpc_message == nullptr) {
        LOG(FATAL) << "RPC message is not set";
    }
    auto rpc = rpc_message;
    rpc_message = nullptr;
    return rpc;
}

/////// HTTP2Connection implementation


HTTP2Connection::HTTP2Connection(std::string conn_host, uint16_t conn_port, ConnectionType conn_type, RPCQueue* rpc_queue, RPCMapper* rpc_mapper,
                                 struct hdr_histogram* hist_struct)
    :   HTTPConnection(conn_host, conn_port, conn_type, hist_struct),
        session(nullptr),
        callbacks(nullptr)
{
    callback_data = std::make_unique<CallbackData>(CallbackData{
        type,
        direction,
        fd,
        rpc_queue,
        rpc_mapper,
        &status,
        hist
    });

    nghttp2_session_callbacks_new(&callbacks);
    set_callbacks(callbacks);
    if (nghttp2_session_client_new(&session, callbacks,
        reinterpret_cast<void*>(callback_data.get())) != 0) {
        LOG(FATAL) << "nghttp2_session_client_new failed";
    }
    
    VLOG(1) << "Created HTTP2Connection for host: " << host << " port: " << port 
            << ", fd: " << fd 
            << ", type: " << type_to_str() 
            << ", direction: " << direction_to_str();
}

HTTP2Connection::HTTP2Connection(int conn_fd, ConnectionType conn_type, RPCMapper* rpc_mapper, RPCQueue* rpc_queue,
                                struct hdr_histogram* hist_struct)
    :   HTTPConnection(conn_fd, conn_type, hist_struct),
        session(nullptr),
        callbacks(nullptr)
{
    callback_data = std::make_unique<CallbackData>(CallbackData{
        type,
        direction,
        fd,
        rpc_queue,
        rpc_mapper,
        &status,
        hist
    });

    // setup nghttp2 session
    nghttp2_session_callbacks_new(&callbacks);
    set_callbacks(callbacks);
    if (nghttp2_session_server_new(&session, callbacks,
            reinterpret_cast<void*>(callback_data.get())) != 0) {
        LOG(FATAL) << "nghttp2_session_server_new failed";
    }

    VLOG(1) << "Created HTTP2Connection for fd: " << fd 
            << ", type: " << type_to_str() 
            << ", direction: " << direction_to_str();
}


void HTTP2Connection::set_callbacks(nghttp2_session_callbacks* callbacks) {
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, frame_recv_callback);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, on_data_chunk_recv_callback);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header_callback);
    nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks, on_begin_headers_callback);
    //nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, on_stream_close_callback);
    nghttp2_session_callbacks_set_on_frame_send_callback(callbacks, on_frame_send_callback);
    nghttp2_session_callbacks_set_error_callback2(callbacks, error_callback);
    nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(callbacks, invalid_frame_callback);
}

void HTTP2Connection::http_read(Buffer* buffer, Ingress& /*ingress*/) {
    VLOG(1) << "Start reading HTTP/2 data on fd: " << fd;
    if (nghttp2_session_mem_recv(session, reinterpret_cast<const uint8_t*>(buffer->data.data()),
        buffer->get_filled()) != (ssize_t)buffer->get_filled()) {
            LOG(FATAL) << "Failed to fully process received HTTP/2 data";
        }
    VLOG(1) << "Finish reading HTTP/2 data on fd: " << fd;
}

bool HTTP2Connection::want_write() {
    return nghttp2_session_want_write(session) != 0;
}

/* bool HTTP2Connection::want_read() {
    return nghttp2_session_want_read(session) != 0;
} */

int HTTP2Connection::http_write(Buffer* buffer) {
    const uint8_t* outbuf_ptr = nullptr;
    ssize_t written = nghttp2_session_mem_send(session, &outbuf_ptr);
    if (written < 0) {
        LOG(FATAL) << "Failed to send HTTP/2 data";
    }

    std::copy_n(outbuf_ptr, written, buffer->data.begin() + (long)buffer->get_filled());
    buffer->set_filled((size_t)written + buffer->get_filled());

    if (buffer->get_filled() > buffer->get_size()) {
        LOG(FATAL) << "Buffer overflow";
    }

    VLOG(1) << "Wrote " << written << " bytes to HTTP/2 connection on fd: " << fd;
    return (int)written;
}

void HTTP2Connection::submit_settings() {
    if (nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, nullptr, 0) != 0 ) {
        LOG(FATAL) << "Failed to submit HTTP/2 settings";
    }
    VLOG(1) << "HTTP/2 settings submitted on fd: " << fd;
}

int32_t HTTP2Connection::submit_request(RPCMessage& rpc) {
    gRPCMessage* grpc = dynamic_cast<gRPCMessage*>(&rpc);
    nghttp2_nv* nva = new nghttp2_nv[grpc->get_req_header_count()];
    auto& req_headers = grpc->get_req_headers();
    for (size_t i = 0; i < grpc->get_req_header_count(); i++) {
        nva[i].name = req_headers.at(i)->name.data();
        nva[i].value = req_headers.at(i)->value.data();
        nva[i].namelen = req_headers.at(i)->name_len;
        nva[i].valuelen = req_headers.at(i)->value_len;
        nva[i].flags = NGHTTP2_NV_FLAG_NONE;
    }

    // preprae the data provider
    nghttp2_data_provider data_prd;
    data_prd.source.ptr = reinterpret_cast<void*>(grpc->get_data_map().at(0));
    data_prd.read_callback = data_read_callback_request;

    // submit the requets and get the upstream stream id
    int32_t id = nghttp2_submit_request(session, nullptr, nva, grpc->get_req_header_count(),
     &data_prd, nullptr);

    if (id < 0) {
        LOG(FATAL) << "Failed to submit HTTP/2 request on fd: " << fd;
    }

    VLOG(1) << "HTTP/2 request submitted on fd: " << fd << " stream id: " << id;
    delete [] nva;
    return id;
}

void HTTP2Connection::submit_response(RPCMessage& rpc) {
    gRPCMessage* grpc = dynamic_cast<gRPCMessage*>(&rpc);
    nghttp2_nv* nva_res = new nghttp2_nv[grpc->get_res_header_count()];
    auto& res_headers = grpc->get_res_headers();
    for (size_t i = 0; i < grpc->get_res_header_count(); i++) {
        nva_res[i].name = res_headers.at(i)->name.data();
        nva_res[i].value = res_headers.at(i)->value.data();
        nva_res[i].namelen = res_headers.at(i)->name_len;
        nva_res[i].valuelen = res_headers.at(i)->value_len;
        nva_res[i].flags = NGHTTP2_NV_FLAG_NONE;
    }


    // preprae the data provider
    nghttp2_data_provider data_prd;
    data_prd.source.ptr = reinterpret_cast<void*>(grpc->get_data_map().at(1));
    data_prd.read_callback = data_read_callback_response;

    if (grpc->get_data_map().at(1)->offset == 0) {
        LOG(WARNING) << "No data to send";
    }

    nghttp2_submit_response(session, rpc.get_ds_stream_id(),
        nva_res, grpc->get_res_header_count(), &data_prd);

    VLOG(1) << "HTTP/2 response submitted on fd: " << fd;
    delete [] nva_res;
}

void HTTP2Connection::submit_error_response(RPCMessage& rpc) {
    gRPCMessage* grpc = dynamic_cast<gRPCMessage*>(&rpc);
    nghttp2_nv* nva_res = new nghttp2_nv[grpc->get_res_header_count()];
    auto& res_headers = grpc->get_res_headers();
    for (size_t i = 0; i < grpc->get_res_header_count(); i++) {
        nva_res[i].name = res_headers.at(i)->name.data();
        nva_res[i].value = res_headers.at(i)->value.data();
        nva_res[i].namelen = res_headers.at(i)->name_len;
        nva_res[i].valuelen = res_headers.at(i)->value_len;
        nva_res[i].flags = NGHTTP2_NV_FLAG_NONE;
    }

    if (nghttp2_submit_response(session, rpc.get_ds_stream_id(),
        nva_res, grpc->get_res_header_count(), nullptr) != 0) {
        LOG(FATAL) << "Failed to submit HTTP/2 error response on fd: " << fd;
    }

    VLOG(1) << "HTTP/2 error response submitted on fd: " << fd;
}

HTTP2Connection::~HTTP2Connection() {
    VLOG(1) << "HTTP2Connection deconstructor on fd: " << fd;
    close(fd);
    if (session) {
        nghttp2_session_del(session);
    }
    if (callbacks) {
        nghttp2_session_callbacks_del(callbacks);
    }
};