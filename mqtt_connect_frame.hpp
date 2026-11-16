// mqtt_connect_frame.hpp
// Purpose: Builds the binary MQTT 3.1.1 CONNECT frame sent by client.cpp (shared with tests.cpp).
#pragma once

#include <vector>
#include <string>
#include <cstdint>

// ============================================================================
// 📦 2. MQTT CONNECT FRAME BUILDER
// ============================================================================
class MqttPacketFactory {
public:
    // Builds a raw binary MQTT 3.1.1 CONNECT frame (clean session, 60 s keep-alive)
    static std::vector<uint8_t> create_connect_frame(const std::string& device_id) {
        std::vector<uint8_t> frame;
        
        // 1. Fixed Header: Packet Type = 0x10 (CONNECT)
        frame.push_back(0x10);

        // 2. Variable Header Payload Data
        std::vector<uint8_t> var_header;
        // Protocol Name Length (0x00, 0x04) + Protocol Name ("MQTT")
        var_header.push_back(0x00); var_header.push_back(0x04);
        var_header.push_back('M');  var_header.push_back('Q'); 
        var_header.push_back('T');  var_header.push_back('T');
        // Protocol Level = 4 (MQTT v3.1.1)
        var_header.push_back(0x04);
        // Connect Flags = 0x02 (Clean Session only)
        var_header.push_back(0x02);
        // Keep Alive Timer = 60 Seconds (0x00, 0x3C)
        var_header.push_back(0x00); var_header.push_back(0x3C);

        // 3. Append Client ID String Payload with big-endian length prefix
        uint16_t id_len = static_cast<uint16_t>(device_id.length());
        var_header.push_back(static_cast<uint8_t>(id_len >> 8));
        var_header.push_back(static_cast<uint8_t>(id_len & 0xFF));
        for (char c : device_id) {
            var_header.push_back(static_cast<uint8_t>(c));
        }

        // 4. Calculate and encode Remaining Length dynamically into Fixed Header
        size_t rem_len = var_header.size();
        do {
            uint8_t encoded_byte = rem_len % 128;
            rem_len /= 128;
            if (rem_len > 0) {
                encoded_byte |= 128;
            }
            frame.push_back(encoded_byte);
        } while (rem_len > 0);

        // 5. Merge variable payload parts into the final serialization vector array
        frame.insert(frame.end(), var_header.begin(), var_header.end());
        return frame;
    }
};
