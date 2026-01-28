#pragma once
#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

namespace json_extract {

// ---------------- file read ----------------
inline bool read_file_bytes(const std::string& path, std::string& out, std::string& err) {
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) {
        err = "failed to open file: " + path;
        return false;
    }
    ifs.seekg(0, std::ios::end);
    out.resize((size_t)ifs.tellg());
    ifs.seekg(0, std::ios::beg);
    if (!out.empty()) ifs.read(out.data(), (std::streamsize)out.size());
    return true;
}

inline void skip_ws(const std::string& in, size_t& i) {
    while (i < in.size()) {
        unsigned char c = (unsigned char)in[i];
        if (c==' '||c=='\t'||c=='\r'||c=='\n') { ++i; continue; }
        break;
    }
}

inline size_t find_quoted_key(const std::string& in, std::string_view key, size_t from) {
    const std::string pat = "\"" + std::string(key) + "\"";
    return in.find(pat, from);
}

// ---------------- JSON string literal -> bytes ----------------
inline int hexv(char c) {
    if ('0'<=c && c<='9') return c-'0';
    if ('a'<=c && c<='f') return 10 + (c-'a');
    if ('A'<=c && c<='F') return 10 + (c-'A');
    return -1;
}

// Parse JSON string literal into bytes; preserves \u00XX control bytes.
// NOTE: if the JSON contains actual replacement char "�" (U+FFFD), it will become EF BF BD (3 bytes) => corrupted.
inline bool parse_json_string_bytes(const std::string& in, size_t& i,
                                    std::vector<uint8_t>& out, std::string& err) {
    if (i >= in.size() || in[i] != '"') { err = "expected '\"' at string start"; return false; }
    ++i;
    out.clear();
    while (i < in.size()) {
        unsigned char c = (unsigned char)in[i++];
        if (c == '"') return true;

        if (c != '\\') { out.push_back((uint8_t)c); continue; }

        if (i >= in.size()) { err = "bad escape at EOF"; return false; }
        unsigned char e = (unsigned char)in[i++];

        switch (e) {
            case '"': out.push_back('"'); break;
            case '\\': out.push_back('\\'); break;
            case '/': out.push_back('/'); break;
            case 'b': out.push_back('\b'); break;
            case 'f': out.push_back('\f'); break;
            case 'n': out.push_back('\n'); break;
            case 'r': out.push_back('\r'); break;
            case 't': out.push_back('\t'); break;
            case 'u': {
                if (i + 4 > in.size()) { err = "bad \\u length"; return false; }
                int h0=hexv(in[i+0]), h1=hexv(in[i+1]), h2=hexv(in[i+2]), h3=hexv(in[i+3]);
                if (h0<0||h1<0||h2<0||h3<0) { err = "bad \\u hex"; return false; }
                uint32_t cp = (uint32_t)((h0<<12)|(h1<<8)|(h2<<4)|h3);
                i += 4;

                if (cp <= 0xFF) {
                    out.push_back((uint8_t)cp);
                } else {
                    // UTF-8 encode for unicode
                    if (cp <= 0x7FF) {
                        out.push_back((uint8_t)(0xC0 | ((cp>>6)&0x1F)));
                        out.push_back((uint8_t)(0x80 | (cp&0x3F)));
                    } else if (cp <= 0xFFFF) {
                        out.push_back((uint8_t)(0xE0 | ((cp>>12)&0x0F)));
                        out.push_back((uint8_t)(0x80 | ((cp>>6)&0x3F)));
                        out.push_back((uint8_t)(0x80 | (cp&0x3F)));
                    } else {
                        out.push_back((uint8_t)(0xF0 | ((cp>>18)&0x07)));
                        out.push_back((uint8_t)(0x80 | ((cp>>12)&0x3F)));
                        out.push_back((uint8_t)(0x80 | ((cp>>6)&0x3F)));
                        out.push_back((uint8_t)(0x80 | (cp&0x3F)));
                    }
                }
                break;
            }
            default:
                err = std::string("unknown escape: \\") + char(e);
                return false;
        }
    }
    err = "unterminated string";
    return false;
}

inline bool extract_string_field(const std::string& in,
                                 std::initializer_list<std::string_view> path,
                                 std::string& out_str,
                                 std::string& err) {
    size_t pos = 0;
    for (auto k : path) {
        pos = find_quoted_key(in, k, pos);
        if (pos == std::string::npos) { err = "key not found: " + std::string(k); return false; }
        pos += k.size() + 2;
    }
    pos = in.find(':', pos);
    if (pos == std::string::npos) { err = "':' not found after path"; return false; }
    ++pos;
    skip_ws(in, pos);
    std::vector<uint8_t> tmp;
    if (!parse_json_string_bytes(in, pos, tmp, err)) return false;
    out_str.assign((const char*)tmp.data(), (const char*)tmp.data() + tmp.size());
    return true;
}

inline bool extract_int64_field(const std::string& in,
                            std::initializer_list<std::string_view> path,
                            std::int64_t& out_value,
                            std::string& err) {
size_t pos = 0;
for (auto k : path) {
    pos = find_quoted_key(in, k, pos);
    if (pos == std::string::npos) { err = "key not found: " + std::string(k); return false; }
    pos += k.size() + 2;
}
pos = in.find(':', pos);
if (pos == std::string::npos) { err = "':' not found after path"; return false; }
++pos;
skip_ws(in, pos);
size_t end = pos;
if (end < in.size() && (in[end] == '-' || in[end] == '+')) ++end;
while (end < in.size()) {
    unsigned char c = static_cast<unsigned char>(in[end]);
    if (c < '0' || c > '9') break;
    ++end;
}
if (end == pos || (end == pos + 1 && (in[pos] == '-' || in[pos] == '+'))) {
    err = "invalid integer value";
    return false;
}
try {
    out_value = std::stoll(in.substr(pos, end - pos));
} catch (const std::exception& e) {
    err = std::string("failed to parse integer: ") + e.what();
    return false;
}
return true;
}

// ---------------- Base64 decode ----------------
inline int b64_index(unsigned char c) {
    if ('A'<=c && c<='Z') return c - 'A';
    if ('a'<=c && c<='z') return c - 'a' + 26;
    if ('0'<=c && c<='9') return c - '0' + 52;
    if (c=='+') return 62;
    if (c=='/') return 63;
    return -1;
}

inline bool base64_decode(std::string_view b64, std::vector<uint8_t>& out, std::string& err) {
    out.clear();
    int val = 0, valb = -8;
    int pad = 0;

    for (unsigned char c : b64) {
        if (c=='=' ) { pad++; continue; }
        if (c=='\r' || c=='\n' || c==' ' || c=='\t') continue;

        int d = b64_index(c);
        if (d < 0) { err = "invalid base64 char"; return false; }

        val = (val << 6) + d;
        valb += 6;
        if (valb >= 0) {
            out.push_back((uint8_t)((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
    // padding is implicitly handled by '=' skipping
    return true;
}

// Detect U+FFFD markers in extracted bytes (EF BF BD)
inline bool contains_ufffd_marker(const std::vector<uint8_t>& v) {
    for (size_t i = 0; i + 2 < v.size(); ++i) {
        if (v[i]==0xEF && v[i+1]==0xBF && v[i+2]==0xBD) return true;
    }
    return false;
}

// ---------------- Public: extract kafka message ----------------
// Priority:
//  1) key.rawPayload / value.rawPayload (base64)  => exact bytes
//  2) fallback to key.payload / value.payload (escaped string) BUT reject if contains U+FFFD marker
inline bool extract_kafka_message(const std::string& json_path,
                                 std::vector<uint8_t>& value_bytes,
                                 std::string& key_text,
                                 std::int64_t& ingest_time_ns,
                                 std::string& err) {
    err.clear();
    std::string raw;
    if (!read_file_bytes(json_path, raw, err)) return false;

    ingest_time_ns = 0;
    std::int64_t ingest_time_ms = 0;
    bool has_ingest_time = extract_int64_field(raw, {"timestamp"}, ingest_time_ms, err);
    if (!has_ingest_time) {
        err.clear();
    } else {
        ingest_time_ns = ingest_time_ms * 1000000LL;
    }

    // try rawPayload first
    std::string key_b64, val_b64;
    bool has_key_b64 = extract_string_field(raw, {"key","rawPayload"}, key_b64, err);
    if (!has_key_b64) err.clear(); // allow missing

    bool has_val_b64 = extract_string_field(raw, {"value","rawPayload"}, val_b64, err);
    if (!has_val_b64) err.clear();

    if (has_key_b64) {
        std::vector<uint8_t> kb;
        if (!base64_decode(key_b64, kb, err)) return false;
        key_text.assign((const char*)kb.data(), (const char*)kb.data() + kb.size());
    } else {
        // fallback to key.payload (text)
        if (!extract_string_field(raw, {"key","payload"}, key_text, err)) return false;
    }

    if (has_val_b64) {
        if (!base64_decode(val_b64, value_bytes, err)) return false;
        return true;
    }

    // fallback to value.payload (dangerous)
    {
        std::vector<uint8_t> vb;
        std::string tmp;
        if (!extract_string_field(raw, {"value","payload"}, tmp, err)) return false;
        vb.assign(tmp.begin(), tmp.end());
        if (contains_ufffd_marker(vb)) {
            err = "value.payload contains U+FFFD replacement markers (bytes corrupted). Please use value.rawPayload base64.";
            return false;
        }
        value_bytes = std::move(vb);
        return true;
    }
}

} // namespace json_extract
