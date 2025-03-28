#include <connection.h>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <liburing.h>
#include <nghttp2/nghttp2.h>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "connection_enums.h"
#include "glog/logging.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include <memory>
#include <sys/types.h>
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

int error_callback(nghttp2_session *session,
                    int lib_error_code,
                    const char *msg,
                    size_t len,
                    void *user_data) {
    LOG(FATAL) << "nghttp2 error (" << lib_error_code << "): " 
              << std::string(msg, len);
    return 0;
}

int invalid_frame_callback(nghttp2_session *session,
                            const nghttp2_frame *frame,
                            int lib_error_code,
                            void *user_data) {
    LOG(WARNING) << "Invalid frame received: " << frame_type_to_str(frame->hd.type);
    return 0;
}

// This callback is used to detect EOS flag for a stream
int frame_recv_callback(nghttp2_session* session,
                        const nghttp2_frame* frame,
                        void* user_data) {
    
    CallbackData* data = reinterpret_cast<CallbackData*>(user_data);

    if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
        DLOG(INFO) << "EOS flag detected on fd: " << data->fd;

        if (frame->hd.type == NGHTTP2_DATA) {
            // we have a request
            DLOG(INFO) << "RPC request received on stream " << frame->hd.stream_id;
            data->queue->enqueue(data->type, data->direction, frame->hd.stream_id);

            // record receive time for the RPC
            data->mapper->get_ds_rpc(data->type, frame->hd.stream_id)->req_rcv_time
             = std::chrono::system_clock::now();
        }
        else if (frame->hd.type == NGHTTP2_HEADERS) {
            // we have a response
            DLOG(INFO) << "RPC response received on stream " << frame->hd.stream_id;
            data->queue->enqueue(data->type, data->direction, frame->hd.stream_id);
            data->mapper->get_us_rpc(data->type, frame->hd.stream_id)->res_rcv_time
             = std::chrono::system_clock::now();
        }
    } else if (frame->hd.type == NGHTTP2_SETTINGS) {
        DLOG(INFO) << "SETTINGS frame received on fd: " << data->fd;
        if (*data->status == ConnectionStatus::DOWN) {
            *data->status = ConnectionStatus::UP;
            DLOG(INFO) << "Connection status changed to UP";
        }
    } else {
        DLOG(INFO) << "Frame type " << frame_type_to_str(frame->hd.type) << " received on fd: " << data->fd;
    }
    return 0;
}

// This callback is used to receive data chunks
int on_data_chunk_recv_callback(nghttp2_session* session,
                                uint8_t flags,
                                int32_t stream_id,
                                const uint8_t* data,
                                size_t len,
                                void* user_data) {

    CallbackData* callback_data = reinterpret_cast<CallbackData*>(user_data);
    
    if (callback_data->direction == ConnectionDirection::DOWNSTREAM) {
        try {
            callback_data->mapper->get_ds_rpc(callback_data->type, stream_id)->add_data(
                data, len, true);
        } catch (const std::out_of_range& e) {
            LOG(FATAL) << "No RPC object found for stream_id: " << stream_id;
        }
    } else {
        try {
            callback_data->mapper->get_us_rpc(callback_data->type, stream_id)->add_data(
                data, len, false);
        } catch (const std::out_of_range& e) {
            LOG(FATAL) << "No RPC object found for stream_id: " << stream_id;
        }
    }

    DLOG(INFO) << "Data chunk received on stream " << stream_id << ": "
    << std::string(reinterpret_cast<const char*>(data), len) << "\n";
    return 0;
}

// This callback is used to receive headers
int on_header_callback(nghttp2_session* session,
                        const nghttp2_frame* frame,
                        const uint8_t* name, size_t namelen,
                        const uint8_t* value, size_t valuelen,
                        uint8_t flags,
                        void* user_data) {

    CallbackData* data = reinterpret_cast<CallbackData*>(user_data);

    if (frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        if (data->direction != ConnectionDirection::DOWNSTREAM) {
            LOG(FATAL) << "Invalid direction for request frame";
        }

        // a request header
        DLOG(INFO) << "Request header received on stream " << frame->hd.stream_id;
        data->mapper->get_ds_rpc(data->type, frame->hd.stream_id)->add_header_field(
            name, namelen, value, valuelen, true, false);
    } else if (frame->headers.cat == NGHTTP2_HCAT_HEADERS) {
        if (data->direction != ConnectionDirection::UPSTREAM) {
            LOG(FATAL) << "Invalid direction for response frame";
        }

        DLOG(INFO) << "tailer header received on stream " << frame->hd.stream_id;
        
        data->mapper->get_us_rpc(data->type, frame->hd.stream_id)->add_header_field(
            name, namelen, value, valuelen, false, true);
    } else if (frame->headers.cat == NGHTTP2_HCAT_RESPONSE) {
        if (data->direction != ConnectionDirection::UPSTREAM) {
            LOG(FATAL) << "Invalid direction for response frame";
        }

        DLOG(INFO) << "Response header received on stream " << frame->hd.stream_id;
        data->mapper->get_us_rpc(data->type, frame->hd.stream_id)->add_header_field(
            name, namelen, value, valuelen, false, false);
    }

    DLOG(INFO) << "Header received: "
    << std::string(reinterpret_cast<const char*>(name), namelen)
    << " : " << std::string(reinterpret_cast<const char*>(value), valuelen);
    return 0;
}

// This callback is used to create the objects to hold request/response
int on_begin_headers_callback(nghttp2_session* session,
                            const nghttp2_frame* frame,
                            void* user_data) {
    
    CallbackData* data = reinterpret_cast<CallbackData*>(user_data);
    if (frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        if (data->direction != ConnectionDirection::DOWNSTREAM) {
            LOG(FATAL) << "Invalid direction for request frame";
        }

        // we have a new request
        DLOG(INFO) << "New request on stream " << frame->hd.stream_id;
        data->mapper->allocate_rpc(data->type, frame->hd.stream_id, data->fd);
    }

    DLOG(INFO) << "Begin headers on stream " << frame->hd.stream_id << " fd: " << data->fd;
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
        std::chrono::system_clock::now() - rpc->get_rcv_time());

    LOG(INFO) << "Latency: " << duration.count() << " us" 
            << " type: " << type_to_str(data->type);
        
    // remove the RPC object
    //data->mapper->remove_rpc(data->type, stream_id);
    DLOG(INFO) << "Stream " << stream_id << " closed on fd: " << data->fd;
    return 0;
} */

int on_frame_send_callback(nghttp2_session *session,
                            const nghttp2_frame *frame,
                            void *user_data) {
    
    CallbackData* data = reinterpret_cast<CallbackData*>(user_data);

    if (frame->headers.cat == NGHTTP2_HCAT_HEADERS
        && data->direction == ConnectionDirection::DOWNSTREAM) {
        DLOG(INFO) << "Returned response";
    }
    
    DLOG(INFO) << "Frame type " << frame_type_to_str(frame->hd.type) << " sent on fd: " << data->fd;
    return 0;
}

ssize_t data_read_callback_request(nghttp2_session*,
                        int32_t /*stream_id*/,
                        uint8_t* buf,
                        size_t length,
                        uint32_t* data_flags,
                        nghttp2_data_source* source,
                        void* /*user_data*/) {
    DLOG(INFO) << "Data provider read callback";
    
    DataReadStruct* info = reinterpret_cast<DataReadStruct*>(source->ptr);

    DLOG(INFO) << "Data: " << std::string(reinterpret_cast<const char*>(info->data), info->len);

    // If the output buffer is too small, copy what fits.
    if (length < info->len) {
        std::memcpy(buf, info->data, length);
        return length;
    } else {
        std::memcpy(buf, info->data, info->len);
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        return info->len;
    }
};

ssize_t data_read_callback_response(nghttp2_session* session,
                            int32_t stream_id,
                            uint8_t* buf,
                            size_t length,
                            uint32_t* data_flags,
                            nghttp2_data_source* source,
                            void* user_data) {
    DLOG(INFO) << "Data provider read callback";

    DataReadStruct* info = reinterpret_cast<DataReadStruct*>(source->ptr);
    CallbackData* callback_data = reinterpret_cast<CallbackData*>(user_data);

    if (info->len == 0) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        DLOG(FATAL) << "No data to send";
        return 0;
    }

    DLOG(INFO) << "Data: " << std::string(reinterpret_cast<const char*>(info->data), info->len);

    // If the output buffer is too small, copy what fits.
    *data_flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
    ssize_t res_len;
    if (length < info->len) {
        std::memcpy(buf, info->data, length);
        //return length;
        res_len = length;
    } else {
        std::memcpy(buf, info->data, info->len);
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        //return info->len;
        res_len = info->len;
    }
    
    auto rpc = callback_data->mapper->get_ds_rpc(callback_data->type, stream_id).get();

    nghttp2_nv* nva_trailers = new nghttp2_nv[rpc->res_trailers.size()];
    for (int i = 0; i < rpc->res_trailers.size(); i++) {
        nva_trailers[i].name = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(rpc->res_trailers[i]->name.c_str()));
        nva_trailers[i].value = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(rpc->res_trailers[i]->value.c_str()));
        nva_trailers[i].namelen = rpc->res_trailers[i]->name_len;
        nva_trailers[i].valuelen = rpc->res_trailers[i]->value_len;
        nva_trailers[i].flags = NGHTTP2_NV_FLAG_NONE;
    }

    if (nghttp2_submit_trailer(session, rpc->ds_stream_id, nva_trailers,
            rpc->res_trailers.size()) != 0) {
        LOG(FATAL) << "Failed to submit HTTP/2 response trailers";
    }

    return res_len;
};

ConnectionPool::ConnectionPool(ConnectionType type) 
    : connections(std::unordered_map<int, std::unique_ptr<HTTPConnection>>()),
      type(type) {};

std::unique_ptr<HTTPConnection>& ConnectionPool::add_connection(std::string& host,
     int port, RPCMapper* mapper, RPCQueue* queue) {
    auto c = std::make_unique<HTTPConnection>(host, port, type, queue, mapper);
    int fd = c->get_fd();
    connections[fd] = std::move(c);
    return connections[fd];
};

bool ConnectionPool::has_connection(int fd) {
    return connections.find(fd) != connections.end();
};

std::unique_ptr<HTTPConnection>& ConnectionPool::get_any_connection() {
    if (connections.empty()) {
        throw NoConnectionException();
    }
    return connections.begin()->second;
};

void HTTPConnection::set_callbacks(nghttp2_session_callbacks* callbacks) {
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, frame_recv_callback);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, on_data_chunk_recv_callback);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header_callback);
    nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks, on_begin_headers_callback);
    //nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, on_stream_close_callback);
    nghttp2_session_callbacks_set_on_frame_send_callback(callbacks, on_frame_send_callback);
    nghttp2_session_callbacks_set_error_callback2(callbacks, error_callback);
    nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(callbacks, invalid_frame_callback);
}

HTTPConnection::HTTPConnection(int fd, ConnectionType type, RPCMapper* mapper, RPCQueue* queue) 
    :   fd(fd),
        type(type),
        addr(0),
        direction(ConnectionDirection::DOWNSTREAM),
        status(ConnectionStatus::UP),
        session(nullptr),
        callbacks(nullptr) {

    callback_data = std::make_unique<CallbackData>(CallbackData{
        type,
        direction,
        fd,
        queue,
        mapper,
        &status
    });
    
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

    // setup nghttp2 session
    nghttp2_session_callbacks_new(&callbacks);
    set_callbacks(callbacks);
    if (nghttp2_session_server_new(&session, callbacks,
            reinterpret_cast<void*>(callback_data.get())) != 0) {
        LOG(FATAL) << "nghttp2_session_server_new failed";
    }
};

HTTPConnection::HTTPConnection(std::string host, int port, ConnectionType type, RPCQueue* queue, RPCMapper* mapper) 
    :   type(type),
        addr(0),
        fd(0),
        direction(ConnectionDirection::UPSTREAM),
        status(ConnectionStatus::DOWN),
        session(nullptr),
        callbacks(nullptr) {

    fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }

    callback_data = std::make_unique<CallbackData>(CallbackData{
        type,
        direction,
        fd,
        queue,
        mapper,
        &status
    });

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        LOG(FATAL) << "Invalid address: " << host;
    }

    int flag = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) == -1) {
        LOG(FATAL) << "Failed to set TCP_NODELAY";
    }

    nghttp2_session_callbacks* callbacks;
    nghttp2_session_callbacks_new(&callbacks);
    set_callbacks(callbacks);
    if (nghttp2_session_client_new(&session, callbacks,
        reinterpret_cast<void*>(callback_data.get())) != 0) {
        LOG(FATAL) << "nghttp2_session_client_new failed";
    } 
}

void HTTPConnection::http_read(Buffer* buffer) {
    DLOG(INFO) << "Start reading HTTP/2 data on fd: " << fd;
    if (nghttp2_session_mem_recv(session, reinterpret_cast<const uint8_t*>(buffer->data.get()),
        buffer->get_filled()) != buffer->get_filled()) {
            LOG(FATAL) << "Failed to fully process received HTTP/2 data";
        }
    DLOG(INFO) << "Finish reading HTTP/2 data on fd: " << fd;
}

bool HTTPConnection::want_write() {
    return nghttp2_session_want_write(session) != 0;
}

bool HTTPConnection::want_read() {
    return nghttp2_session_want_read(session) != 0;
}

int HTTPConnection::http_write(Buffer* buffer) {
    const uint8_t* outbuf_ptr = nullptr;
    int written = nghttp2_session_mem_send(session, &outbuf_ptr);
    if (written < 0) {
        LOG(FATAL) << "Failed to send HTTP/2 data";
    }

    std::memcpy(buffer->data.get(), outbuf_ptr, written);
    buffer->set_filled(written);

    DLOG(INFO) << "HTTP/2 data written on fd: " << fd;
    return written;
}

void HTTPConnection::submit_settings() {
    if (nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, nullptr, 0) != 0 ) {
        LOG(FATAL) << "Failed to submit HTTP/2 settings";
    }
    DLOG(INFO) << "HTTP/2 settings submitted on fd: " << fd;
}

int32_t HTTPConnection::submit_request(RPCMessage& rpc) {
    nghttp2_nv* nva = new nghttp2_nv[rpc.req_headers.size()];
    for (int i = 0; i < rpc.req_headers.size(); i++) {
        nva[i].name = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(rpc.req_headers[i]->name.c_str()));
        nva[i].value = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(rpc.req_headers[i]->value.c_str()));
        nva[i].namelen = rpc.req_headers[i]->name_len;
        nva[i].valuelen = rpc.req_headers[i]->value_len;
        nva[i].flags = NGHTTP2_NV_FLAG_NONE;
    }

    // preprae the data provider
    nghttp2_data_provider data_prd;
    data_prd.source.ptr = reinterpret_cast<void*>(&rpc.req_data);
    data_prd.read_callback = data_read_callback_request;

    // submit the requets and get the upstream stream id
    int32_t id = nghttp2_submit_request(session, nullptr, nva, rpc.req_headers.size(),
     &data_prd, nullptr);

    if (id < 0) {
        LOG(FATAL) << "Failed to submit HTTP/2 request on fd: " << fd;
    }

    DLOG(INFO) << "HTTP/2 request submitted on fd: " << fd;
    return id;
}

void HTTPConnection::submit_response(RPCMessage& rpc) {
    nghttp2_nv* nva_res = new nghttp2_nv[rpc.res_headers.size()];
    for (int i = 0; i < rpc.res_headers.size(); i++) {
        nva_res[i].name = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(rpc.res_headers[i]->name.c_str()));
        nva_res[i].value = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(rpc.res_headers[i]->value.c_str()));
        nva_res[i].namelen = rpc.res_headers[i]->name_len;
        nva_res[i].valuelen = rpc.res_headers[i]->value_len;
        nva_res[i].flags = NGHTTP2_NV_FLAG_NONE;
    }

    nghttp2_nv* nva_trailers = new nghttp2_nv[rpc.res_trailers.size()];
    for (int i = 0; i < rpc.res_trailers.size(); i++) {
        nva_trailers[i].name = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(rpc.res_trailers[i]->name.c_str()));
        nva_trailers[i].value = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(rpc.res_trailers[i]->value.c_str()));
        nva_trailers[i].namelen = rpc.res_trailers[i]->name_len;
        nva_trailers[i].valuelen = rpc.res_trailers[i]->value_len;
        nva_trailers[i].flags = NGHTTP2_NV_FLAG_NONE;
    }

    // preprae the data provider
    nghttp2_data_provider data_prd;
    data_prd.source.ptr = reinterpret_cast<void*>(&rpc.res_data);
    data_prd.read_callback = data_read_callback_response;

    if (rpc.res_data.len == 0) {
        LOG(FATAL) << "No data to send";
    }

    nghttp2_submit_response(session, rpc.ds_stream_id,
        nva_res, rpc.res_headers.size(), &data_prd);

    DLOG(INFO) << "HTTP/2 response submitted on fd: " << fd;
}

sockaddr* HTTPConnection::get_addr() {
    return reinterpret_cast<sockaddr*>(&addr);
}

HTTPConnection::~HTTPConnection() {
    DLOG(INFO) << "HTTPConnection deconstructor on fd: " << fd;
    close(fd);
    if (session) {
        nghttp2_session_del(session);
    }
    if (callbacks) {
        nghttp2_session_callbacks_del(callbacks);
    }
};

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