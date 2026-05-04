#include "buffer.h"
#include "connection_enums.h"
#include "listener.h"
#include "ring_helper.hpp"
#include "state.h"
#include <buffer_manager.h>
#include <cassert>
#include <connection.h>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <event_loop.h>
#include <glog/logging.h>
#include <iostream>
#include <memory>
#include <ring_wrapper.h>
#include <string>
#include <strings.h>
#include <sys/socket.h>

void EventLoop::run() {
  // Pointers to accept and identify completion events
  struct io_uring_cqe *cqe;
  UserData *ud;

  // Add accept submissions
  for (auto &[type, listener] : listeners) {
    VLOG(1) << "Listening on " << listener->type_to_str()
            << " listener, port: " << listener->get_port();
    ring.prepare_accept(listener, buffer_manager.get_user_data());
  }

  ring.prepare_rcvmsg(state.get_sockfd(), buffer_manager.get_user_data());

  // main event loop
  while (true) {
    ring.submit_and_wait();

    while ((cqe = ring.peek_cqe()) != nullptr) {
      VLOG(1) << "Processing completion event";

      // Identify the type of event
      ud = ring.get_user_data(cqe);

      // Handle the event
      switch (ud->op) {
      case Operation::ACCEPT: {
        VLOG(1) << "Accept completion event";
        // get the listener from the user data
        auto listener = ud->listener;

        if (cqe->res < 0) {
          LOG(FATAL) << "Failed to accept connection, error: "
                     << strerror(-cqe->res);
          // break;
        } else {
          // add the connection to the listener
          HTTP http_type;
          if (config.is_frontend && listener->type == ConnectionType::INGRESS) {
            http_type = HTTP::HTTP1;
          } else if (config.is_ingress) {
            http_type = HTTP::HTTP1;
          } else {
            http_type = HTTP::HTTP2;
          }

          auto conn = listener->add_connection(
              cqe->res, std::addressof(rpc_mapper), std::addressof(rpc_queue),
              http_type, std::addressof(state.stats));

          VLOG(1) << "Index:" << index << " accepted connection on "
                  << listener->type_to_str() << " listener"
                  << " fd: " << listener->get_fd();
          VLOG(1) << "New connection on fd: " << conn->get_fd();

          // submit the first setting frame
          conn->submit_settings();

          // prepare the first read
          ring.prepare_read(buffer_manager.get_user_data(), listener,
                            std::move(conn));
        }

        // re-arm accept on the listener
        ring.prepare_accept(std::move(listener),
                            buffer_manager.get_user_data());
        // free the user data
        buffer_manager.free_user_data(ud);
        break;
      }

      case Operation::READ: {
        VLOG(1) << "Read completion event";
        // get the buffer from the user data
        std::unique_ptr<Buffer> buffer = nullptr;
        if (cqe->flags & IORING_CQE_F_BUFFER) {
          auto buffer_index = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
          buffer = buffer_manager.get_buffer_by_index(buffer_index);
          if (!buffer) {
            LOG(FATAL) << "Buffer is null in read completion event";
          }
        } else if (cqe->res > 0) {
          // If we read data but got no (provided) buffer, that's an issue for
          // multishot
          LOG(FATAL) << "Read " << cqe->res
                     << " bytes but no buffer provided (flag missing)";
        }

        // get the original connection and listener
        auto orig_conn = ud->conn;
        auto orig_listener = ud->listener;

        // log the read event
        VLOG(1) << "fd: " << orig_conn->get_fd();
        VLOG(1) << "Read " << cqe->res << " bytes from buffer";
        VLOG(1) << "Connection type: " << orig_conn->type_to_str();
        VLOG(1) << "Connection direction: " << orig_conn->direction_to_str();

        // check corner cases (errors, closed connection)
        if (cqe->res <= 0) {
          if (cqe->res < 0) {
            state.dump_entire_state();
            LOG(FATAL) << "Failed to read from " << orig_conn->get_host() << ":"
                       << orig_conn->get_port()
                       << ", error: " << strerror(-cqe->res);
          } else if (cqe->res == 0) {
            LOG(INFO) << "Closing connection on fd: " << orig_conn->get_fd()
                      << " of type: " << orig_conn->type_to_str()
                      << " and direction: " << orig_conn->direction_to_str();
            /* if (config.is_ingress &&
            rpc_mapper.check_fd_exists(orig_conn->type, orig_conn->get_fd(),
            false)) { listeners.at(orig_conn->type)->dump_connections();
                state.dump_entire_state();
                LOG(FATAL) << "FD exists in ds map for fd: " <<
            orig_conn->get_fd()
                           << " of type: " << orig_conn->type_to_str();
            } */
          }

          // update connection status
          orig_conn->set_status(ConnectionStatus::TEARDOWN);

          // free buffer
          if (buffer) {
            buffer_manager.free_buffer(std::move(buffer));
          }

          // prepare cancel
          ring.prepare_cancel(std::move(orig_conn),
                              buffer_manager.get_user_data());
          break;
        }
        buffer->set_filled((size_t)cqe->res);

        if (!orig_conn) {
          LOG(FATAL) << "orig_conn is null";
        }
        try {
          orig_conn->http_read(buffer, ingress);
        } catch (const HTTPParseException &e) {
          LOG(WARNING) << "HTTP Parse exception: " << e.what();
          orig_conn->set_status(ConnectionStatus::TEARDOWN);
          if (buffer) {
            buffer_manager.free_buffer(std::move(buffer));
          }
          ring.prepare_cancel(std::move(orig_conn),
                              buffer_manager.get_user_data());
          break;
        }
        assert(orig_conn != nullptr && "orig_conn should not be null here");

        // send out http2-related data
        state.write_http(orig_conn);

        // free the buffer
        buffer_manager.free_buffer(std::move(buffer));

        // handle req/res send buffers
        state.forward(orig_conn->type, orig_conn->direction);

        // check ingress admission
        if (config.is_ingress) {
          state.ingress_pre_credit();
        }

        try {
          state.ppm_client(false, nullptr);
        } catch (const std::out_of_range &e) {
          LOG(FATAL) << "Out of range error: " << e.what();
        } catch (const std::exception &e) {
          LOG(FATAL) << "Error in PPM client: " << e.what();
        }

        // flush every HTTP2 frame out
        state.write_http(orig_conn);

        break;
      }

      case Operation::CONNECT: {
        // check if the connection is successful
        if (cqe->res < 0) {
          LOG(FATAL) << "Failed to connect to fd: " << ud->conn->get_fd()
                     << ", error: " << strerror(-cqe->res);
          break;
        }

        auto orig_conn = std::move(ud->conn);

        VLOG(1) << "Connect completion event, fd: " << orig_conn->get_fd();

        // submit first SETTING frame
        orig_conn->submit_settings();
        VLOG(1) << "conn type: " << orig_conn->type_to_str();
        VLOG(1) << "conn direction: " << orig_conn->direction_to_str();

        if (orig_conn->http() == HTTP::HTTP1) {
          orig_conn->set_status(ConnectionStatus::UP);
        } else if (orig_conn->http() == HTTP::HTTP2) {
          // write http frames (initial settings, which acts as handshake). Only
          // for HTTP/2 connections
          state.write_http(orig_conn);
        }

        // arm the first read
        ring.prepare_read(buffer_manager.get_user_data(),
                          listeners.at(orig_conn->type), orig_conn);

        // free the user data
        buffer_manager.free_user_data(ud);
        break;
      }

      case Operation::WRITE: {
        VLOG(1) << "Write completion event";
        VLOG(1) << "Wrote " << cqe->res
                << " bytes to fd: " << ud->conn->get_fd();

        if (cqe->res < 0) {
          LOG(FATAL) << "Failed to write to fd: " << ud->conn->get_fd()
                     << ", error: " << strerror(-cqe->res);
        }

        // free buffer
        buffer_manager.free_buffer(ud->get_buffer());

        // free the user data
        buffer_manager.free_user_data(ud);

        break;
      }

      case Operation::CANCEL: {
        auto conn = std::move(ud->conn);
        VLOG(1) << "Cancel completion event, fd: " << conn->get_fd();

        switch (conn->direction) {
        case ConnectionDirection::UPSTREAM:
          // for upstream connections, pools (inside state) hold the connection
          state.remove_connection(std::move(conn));
          break;
        case ConnectionDirection::DOWNSTREAM:
          // also remove the corresponsing connection from state becasue,
          // we don't need to route the response to/from this connection
          /* ring.prepare_cancel(
              state.get_one_connection(conn.type),
              buffer_manager.get_user_data()
          ); */
          // check if the rpc mapper is empty
          if (rpc_mapper.check_fd_exists(conn->type, conn->get_fd(), false)) {
            // state.dump_entire_state();
            LOG(FATAL) << "RPC mapper is not empty for fd: " << conn->get_fd()
                       << " of type: " << conn->type_to_str()
                       << " when removing connection"
                       << " (Maybe a TIMEOUT occurred?)";
          }
          // remove the object of the connection from the listener
          listeners.at(conn->type)->remove_connection(conn->get_fd());
          conn.reset();
          break;
        default:
          LOG(FATAL) << "Unknown connection direction";
        }

        // free the user data
        buffer_manager.free_user_data(ud);

        break;
      }

      case Operation::RCVMSG: {
        VLOG(1) << "Recvmsg completion event";

        // get the buffer from the user data
        std::unique_ptr<Buffer> old_buffer = nullptr;
        if (cqe->flags & IORING_CQE_F_BUFFER) {
          auto buffer_index = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
          old_buffer = buffer_manager.get_dn_buffer_by_index(buffer_index);
          if (!old_buffer) {
            LOG(FATAL) << "DN Buffer is null in read completion event";
          }
        } else if (cqe->res > 0) {
          // If we read data but got no (provided) buffer, that's an issue for
          // multishot
          LOG(FATAL) << "Read " << cqe->res
                     << " bytes but no DN buffer provided (flag missing)";
        }
        if (cqe->res <= 0) {
          LOG(FATAL) << "Failed to receive UDP message, error: "
                     << strerror(-cqe->res);
        }

        RingWrapper::handle_multishot_recv(old_buffer, cqe->res);

        VLOG(1) << "PPM UDP recv, dispatch by header";
        try {
          state.dispatch_ppm_recv(old_buffer);
        } catch (const std::out_of_range &e) {
          LOG(FATAL) << "Out of range error: " << e.what();
        } catch (const std::exception &e) {
          LOG(FATAL) << "Error in PPM dispatch: " << e.what();
        }

        buffer_manager.free_dn_buffer(std::move(old_buffer));

        break;
      }

      case Operation::SENDMSG: {
        VLOG(1) << "Sendmsg completion event";
        auto buffer = ud->get_buffer();

        if (cqe->res <= 0) {
          LOG(FATAL) << "Failed to send message"
                     << " res: " << cqe->res
                     << " error: " << strerror(-cqe->res)
                     << " udp_type: " << udp_type_to_str(ud->udp_type)
                     << " buffer content: "
                     << std::string(buffer->data.begin(),
                                    buffer->data.begin() +
                                        (long)buffer->get_filled());
        }

        // free the buffer
        buffer_manager.free_dn_buffer(std::move(buffer));

        // free the user data
        buffer_manager.free_user_data(ud);

        break;
      }

      case Operation::CLEAR: {
        LOG(FATAL) << "Received a CLEAR operation, index: " << ud->index;
        break;
      }

      default:
        LOG(FATAL) << "Unknown operation: " << static_cast<int>(ud->op);
        break;
      }

      // Advance the ring
      ring.seen_cqe(cqe);
    }
  }
};

EventLoop::EventLoop(int th_index, std::string &ingress_service_ref,
                     Config parsed_config, SharedState &shared_state)
    : index(th_index), ingress_service(ingress_service_ref),
      config(parsed_config), ring(config.ring_size),
      buffer_manager(config.buffer_count, config.buffer_size, ring),
      listeners(), rpc_mapper(), rpc_queue(),
      stats(get_downstream_services(parsed_config),
            get_hosted_services(parsed_config)),
      ingress(th_index, ingress_service_ref, stats),
      state(config, ring, buffer_manager, rpc_mapper, rpc_queue, listeners,
            ingress, shared_state, ingress_service_ref, th_index, stats) {

  uint16_t egress_port;
  if (config.is_ingress) {
    egress_port =
        config.mapping.at(ingress_service_ref).listen_port.value_or(0);
    if (egress_port == 0) {
      LOG(FATAL) << "Egress port is not set for ingress service: "
                 << ingress_service_ref;
    }
  } else {
    egress_port = config.egress_listener_port;
  }
  listeners.emplace(
      ConnectionType::EGRESS,
      std::make_shared<Listener>(egress_port, ConnectionType::EGRESS));

  listeners.emplace(ConnectionType::INGRESS,
                    std::make_shared<Listener>(config.ingress_listener_port,
                                               ConnectionType::INGRESS));
};