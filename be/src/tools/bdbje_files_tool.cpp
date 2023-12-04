#include <unordered_map>
#include <iostream>
#include <memory>
#include <vector>
#include <fstream>
#include <string>
#include <sstream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

class ParserHelper {
public:
    static uint64_t read_uint64(const std::vector<char>& buf, int pos) {
        uint64_t res = ((uint64_t)buf[pos++] & 0xFFL);
        res += (((uint64_t)buf[pos++] & 0xFFL) << 8);
        res += (((uint64_t)buf[pos++] & 0xFFL) << 16);
        res += (((uint64_t)buf[pos++] & 0xFFL) << 24);
        res += (((uint64_t)buf[pos++] & 0xFFL) << 32);
        res += (((uint64_t)buf[pos++] & 0xFFL) << 40);
        res += (((uint64_t)buf[pos++] & 0xFFL) << 48);
        res += (((uint64_t)buf[pos++] & 0xFFL) << 56);
        return res;
    }
    static uint32_t read_uint32(const std::vector<char>& buf, int pos) {
        uint32_t res = (buf[pos++] & 0xFF);
        res += ((buf[pos++] & 0xFF) << 8);
        res += ((buf[pos++] & 0xFF) << 16);
        res += ((buf[pos++] & 0xFF) << 24);
        return res;
    }
    static uint16_t read_uint16(const std::vector<char>& buf, int pos) {
        uint16_t res = (buf[pos++] & 0xFF);
        res += ((buf[pos++] & 0xFF) << 8);
        return res;
    }
    static uint8_t read_uint8(const std::vector<char>& buf, int pos) {
        return buf[pos];
    }

    static uint64_t read_user_key(const std::vector<char>& buf, int pos) {
        uint64_t result = 0;
        for (int i = 0; i < 8; ++i) {
            result <<= 8;
            result |= (buf[pos + i] & 0xFFL);
        }
        return result ^ 0x8000000000000000L;
    }
    static int16_t read_short(const std::vector<char>& buf, int pos) {
        if (pos + 1 >= buf.size()) {
            return 0xFFFF;
        }
        uint16_t res = ((buf[pos++] & 0xFF) << 8);
        res += (buf[pos++] & 0xFF);
        return res;
    }

    static int get_packed_length(const std::vector<char>& buf, int off) {
        int b1 = buf[off];
        if (b1 < -119) {
            return -b1 - 119 + 1;
        } else if (b1 > 119) {
            return b1 - 119 + 1;
        } else {
            return 1;
        }
    }
    static int32_t read_packed_int32(const std::vector<char>& buf, int off) {
        bool negative;
        int byteLen;
        int b1 = buf[off++];
        if (b1 < -119) {
            negative = true;
            byteLen = -b1 - 119;
        } else if (b1 > 119) {
            negative = false;
            byteLen = b1 - 119;
        } else {
            return b1;
        }

        int value = buf[off++] & 0xFF;
        if (byteLen > 1) {
            value |= (buf[off++] & 0xFF) << 8;
            if (byteLen > 2) {
                value |= (buf[off++] & 0xFF) << 16;
                if (byteLen > 3) {
                    value |= (buf[off++] & 0xFF) << 24;
                }
            }
        }

        return negative ? (-value - 119) : (value + 119);
    }
    static int64_t read_packed_int64(const std::vector<char>& buf, int off) {
        bool negative;
        int byteLen;
        int b1 = buf[off++];
        if (b1 < -119) {
            negative = true;
            byteLen = -b1 - 119;
        } else if (b1 > 119) {
            negative = false;
            byteLen = b1 - 119;
        } else {
            return b1;
        }

        int64_t value = (int64_t)buf[off++] & 0xFFL;
        if (byteLen > 1) {
            value |= ((int64_t)buf[off++] & 0xFFL) << 8;
            if (byteLen > 2) {
                value |= ((int64_t)buf[off++] & 0xFFL) << 16;
                if (byteLen > 3) {
                    value |= ((int64_t)buf[off++] & 0xFFL) << 24;
                    if (byteLen > 4) {
                        value |= ((int64_t)buf[off++] & 0xFFL) << 32;
                        if (byteLen > 5) {
                            value |= ((int64_t)buf[off++] & 0xFFL) << 40;
                            if (byteLen > 6) {
                                value |= ((int64_t)buf[off++] & 0xFFL) << 48;
                                if (byteLen > 7) {
                                    value |= ((int64_t)buf[off++] & 0xFFL) << 56;
                                }
                            }
                        }
                    }
                }
            }
        }

        int64_t result = negative ? (-value - 119) : (value + 119);
/*
        std::cout << "-value=" << -value << std::endl;
        std::cout << "value=" << value << "len=" << (int)buf[0] << " value_hex=" << std::hex << value << ", len_hex=" << ((int)buf[0]&0xFF) << ", real_len=" << byteLen << ", ";
        for (int i = 1; i < off; ++i) {
            std::cout << ((uint16_t)buf[i] & 0xFF) << ", ";
        }
        std::cout << std::dec << std::endl;
*/
        return result;
    }

    static std::string LSN_to_string(int64_t lsn) {
        std::stringstream ss;
        ss << "0x" << std::hex << ((lsn >> 32) & 0xFFFFFFFF) << std::dec;
        ss << "/" << (lsn & 0xFFFFFFFF);
        return ss.str();
    }
};

class JDBLogHeader {
public:
    static const int MIN_LOG_HEADER_SIZE = 14;
    static const int EXTRA_HEADER_SIZE = 8;

    uint32_t check_sum;
    uint8_t entry_type;
    uint8_t entry_flag;
    uint32_t prev_offset;
    uint32_t item_size;

    uint64_t vlsn;
public:
    bool set_header(const std::vector<char>& data) {
        check_sum = ParserHelper::read_uint32(data, 0);
        entry_type = ParserHelper::read_uint8(data, 4);
        entry_flag = ParserHelper::read_uint8(data, 5);
        prev_offset = ParserHelper::read_uint32(data, 6);
        item_size = ParserHelper::read_uint32(data, 10);
        return true;
    }
    bool need_extra_header() const {
        return (entry_flag & 0x20) || (entry_flag & 0x08);
    }
    bool set_extra_header(const std::vector<char>& data) {
        vlsn = ParserHelper::read_uint64(data, 0);
        return true;
    }
    uint32_t get_data_size() const {
        return item_size;
    }
    uint8_t get_entry_type() const {
        return entry_type;
    }

    std::string to_string() const {
        std::stringstream ss;
        ss << "[prev_off=" << prev_offset;
        ss << ", type=" << (uint32_t)entry_type;
        //ss << ", data_size=" << item_size;
        if (need_extra_header()) {
            ss << ", vlsn=" << vlsn;
        }
        ss << "]";
        return ss.str();
    }
};

class JDBLogDataBase {
public:
    virtual bool set_data(const std::vector<char>& d) {
        data = d;
        return true;
    }
    virtual std::string to_string() const {
        std::stringstream ss;
        ss << "[data_size=" << data.size();
        ss << "]";
        return ss.str();
    }

public:
    std::vector<char> data;
};
class JDBLogFileHeader : public JDBLogDataBase {
public:
    bool set_data(const std::vector<char>& d) override {
        JDBLogDataBase::set_data(d);

        _tm = ParserHelper::read_uint64(data, 0);
        _file_num = ParserHelper::read_uint32(data, 8);
        _prev_file_off = ParserHelper::read_uint64(data, 12);
        _log_version = ParserHelper::read_uint32(data, 20);
        return true;
    }
    std::string to_string() const override {
        std::stringstream ss;
        ss << "[FileHeader";
        ss << " tm=" << _tm / 1000;
        ss << " file=" << std::hex << _file_num << std::dec;
        ss << " prev_off=" << _prev_file_off;
        ss << " version=" << _log_version;
        ss << "]";
        return ss.str();
    }

private:
    uint64_t _tm;
    uint32_t _file_num;
    uint64_t _prev_file_off;
    uint32_t _log_version;
};
class JDBLogCommit : public JDBLogDataBase {
public:
    bool set_data(const std::vector<char>& d) override {
        JDBLogDataBase::set_data(d);

        int pos = 0;
        _last_lsn = ParserHelper::read_packed_int64(data, pos);
        pos += ParserHelper::get_packed_length(data, pos);

        _id = ParserHelper::read_packed_int64(data, pos);
        pos += ParserHelper::get_packed_length(data, pos);

        _tm = ParserHelper::read_packed_int64(data, pos);
        pos += ParserHelper::get_packed_length(data, pos);

        _master_node = ParserHelper::read_packed_int32(data, pos);
        pos += ParserHelper::get_packed_length(data, pos);

        _dtv_lsn = ParserHelper::read_packed_int64(data, pos);
        pos += ParserHelper::get_packed_length(data, pos);
        return true;
    }

    std::string to_string() const override {
        std::stringstream ss;
        ss << "[Commit last_lsn=" << ParserHelper::LSN_to_string(_last_lsn);
        ss << " id=" << std::hex << _id << std::dec;
        ss << " tm=" << _tm / 1000;
        ss << " master=" << _master_node;
        ss << " dtv_lsn=" << ParserHelper::LSN_to_string(_dtv_lsn);
        ss << "]";
        return ss.str();
    }

private:
    uint64_t _last_lsn;
    uint64_t _id;
    uint64_t _tm;
    uint32_t _master_node;
    uint64_t _dtv_lsn;
};
class JDBLogUserLN : public JDBLogDataBase {
public:
    bool set_data(const std::vector<char>& d) override {
        JDBLogDataBase::set_data(d);

        _flag = ParserHelper::read_uint8(data, 0);
        _flag2 = ParserHelper::read_uint8(data, 1);

        int pos = 2;
        _dbid = ParserHelper::read_packed_int64(data, pos);
        pos += ParserHelper::get_packed_length(data, pos);

        if (has_abort_lsn()) {
            _abort_lsn = ParserHelper::read_packed_int64(data, pos);
            pos += ParserHelper::get_packed_length(data, pos);
        }

        _txn_id = ParserHelper::read_packed_int64(data, pos);
        pos += ParserHelper::get_packed_length(data, pos);

        _txn_last_lsn = ParserHelper::read_packed_int64(data, pos);
        pos += ParserHelper::get_packed_length(data, pos);

        size_t user_data_size = ParserHelper::read_packed_int32(data, pos);
        pos += ParserHelper::get_packed_length(data, pos);

        _user_data = {d.begin() + pos, d.begin() + pos + user_data_size};
        pos += user_data_size;

        if (d.size() != pos + 8) {
            std::cout << "Wrong Key size != 8" << std::endl;
            return false;
        }
        _key = ParserHelper::read_user_key(data, pos);
        //std::cout << "pos=" << pos << ", total=" << d.size() << std::endl;
        return true;
    }
    bool has_abort_lsn() const {
        return _flag & 0x20;
    }
    std::string to_string() const override {
        std::stringstream ss;
        ss << "[UserLN op=" << (int)ParserHelper::read_short(_user_data, 0);
        ss << " key=" << _key;
        ss << " dbid=" << _dbid;
        ss << " dbid_hex=" << std::hex << _dbid << std::dec;
        ss << " txnid=" << _txn_id;
        if (has_abort_lsn()) {
            ss << " abort_lsn=" << ParserHelper::LSN_to_string(_abort_lsn);
        }
        ss << "]";
        return ss.str();
    }

private:
    uint8_t _flag;
    uint8_t _flag2;
    int64_t _dbid;
    int64_t _abort_lsn;
    int64_t _txn_id;
    int64_t _txn_last_lsn;
    std::vector<char> _user_data;
    uint64_t _key;
};


class JDBLogDataParser {
public:
    static std::shared_ptr<JDBLogDataBase> get_data_handler(int type) {
        if (type == 25) {
            return std::make_shared<JDBLogFileHeader>();
        } else if (type == 17) {
            return std::make_shared<JDBLogCommit>();
        } else if (type == 33) {
            return std::make_shared<JDBLogUserLN>();
        } else {
            return std::make_shared<JDBLogDataBase>();
        }
    }
};
class JDBFileParser {
public:
    JDBFileParser() {
        _setup_wrong_logs();
    }

    bool init(const std::string& file_name) {
        _jdb_file.open(file_name, std::ios::binary);
        if (!_jdb_file) {
            return false;
        }
        return true;
    }

    size_t cur_off() {
        return _jdb_file.tellg();
    }

    bool read_next_log() {
        cur_offset = cur_off();
        if (_is_wrong_log()) {
            _skip_wrong_log();
        }

        // read header
        std::vector<char> buf(JDBLogHeader::MIN_LOG_HEADER_SIZE);
        if(!_jdb_file.read(buf.data(), buf.size())) {
            return false;
        }
        if (!header.set_header(buf)) {
            return false;
        }

        if (header.need_extra_header()) {
            buf.resize(JDBLogHeader::EXTRA_HEADER_SIZE);
            if(!_jdb_file.read(buf.data(), buf.size())) {
                return false;
            }
            if (!header.set_extra_header(buf)) {
                return false;
            }
        }

        // read data
        auto data_size = header.get_data_size();
        buf.resize(data_size);
        if (!_jdb_file.read(buf.data(), buf.size())) {
            return false;
        }
        data = JDBLogDataParser::get_data_handler(header.get_entry_type());
        if (!data->set_data(buf)) {
            return false;
        }

        return true;
    }

    void display() const {
        std::cout << cur_offset << ":";
        if (_is_wrong_log()) {
            std::cout << "wrong log";
        } else {
            std::cout << header.to_string() << " | " << data->to_string();
        }
        std::cout << std::endl;
    }
    
public:
    int64_t cur_offset;
    JDBLogHeader header;
    std::shared_ptr<JDBLogDataBase> data;

private:
    void _setup_wrong_logs() {
        _wrong_log_offsets.insert({2682004L, 2683163L});
    }
    bool _is_wrong_log() const {
        return _wrong_log_offsets.find(cur_offset) != _wrong_log_offsets.end();
    }
    void _skip_wrong_log() {
        if (_is_wrong_log()) {
            _jdb_file.seekg(_wrong_log_offsets[cur_offset], std::ios::beg);
        }
    }
    std::unordered_map<int64_t, int64_t> _wrong_log_offsets;

private:
    std::ifstream _jdb_file;
};


/*
uint64_t read_uint64(int8_t* buf) {
    uint64_t result = 0;
    for (int i = 0; i < 8; ++i) {
        result <<= 8;
        result |= (buf[i] & 0xFFL);
    }
    return result ^ 0x8000000000000000L;
}
int16_t read_short(int8_t* buf) {
    return ((buf[0]) << 8) | (buf[1] & 0xFF);
}

int get_read_int_length(int8_t* buf) {
    int b1 = *buf;
    if (b1 < -119) {
        return -b1 - 119 + 1;
    } else if (b1 > 119) {
        return b1 - 119 + 1;
    } else {
        return 1;
    }
}

int32_t read_int(int8_t* buf) {
    bool negative;
    int byteLen;
    int off = 0;

    int b1 = buf[off++];
    if (b1 < -119) {
        negative = true;
        byteLen = -b1 - 119;
    } else if (b1 > 119) {
        negative = false;
        byteLen = b1 - 119;
    } else {
        return b1;
    }

    int value = buf[off++] & 0xFF;
    if (byteLen > 1) {
        value |= (buf[off++] & 0xFF) << 8;
        if (byteLen > 2) {
            value |= (buf[off++] & 0xFF) << 16;
            if (byteLen > 3) {
                value |= (buf[off++] & 0xFF) << 24;
            }
        }
    }

    return negative ? (-value - 119) : (value + 119);
}

    int64_t read_long(int8_t* buf) {
        int off = 0;
        bool negative;
        int byteLen;
        int b1 = buf[off++];
        if (b1 < -119) {
            negative = true;
            byteLen = -b1 - 119;
        } else if (b1 > 119) {
            negative = false;
            byteLen = b1 - 119;
        } else {
            return b1;
        }

        int64_t value = buf[off++] & 0xFFL;
        if (byteLen > 1) {
            value |= (buf[off++] & 0xFFL) << 8;
            if (byteLen > 2) {
                value |= (buf[off++] & 0xFFL) << 16;
                if (byteLen > 3) {
                    value |= (buf[off++] & 0xFFL) << 24;
                    if (byteLen > 4) {
                        value |= (buf[off++] & 0xFFL) << 32;
                        if (byteLen > 5) {
                            value |= (buf[off++] & 0xFFL) << 40;
                            if (byteLen > 6) {
                                value |= (buf[off++] & 0xFFL) << 48;
                                if (byteLen > 7) {
                                    value |= (buf[off++] & 0xFFL) << 56;
                                }
                            }
                        }
                    }
                }
            }
        }

        int64_t result = negative ? (-value - 119) : (value + 119);
        std::cout << "-value=" << -value << std::endl;
        std::cout << "value=" << value << "len=" << (int)buf[0] << " value_hex=" << std::hex << value << ", len_hex=" << ((int)buf[0]&0xFF) << ", real_len=" << byteLen << ", ";
        for (int i = 1; i < off; ++i) {
            std::cout << ((uint16_t)buf[i] & 0xFF) << ", ";
        }
        std::cout << std::dec << std::endl;
        return result;
    }

class LogEntity {
public:
    bool set_data(int8_t* buf, int size, uint8_t entry_type) {
        if (size <= 0) {
            return true;
        }
        if (entry_type == 17) {
            int total_len = 0;

            int len = get_read_int_length(buf);
            total_len += len;
            int64_t lastLsn = read_long(buf);
            buf += len;
            std::cout << "lastLsn = " << lastLsn << ", len=" << total_len << ", size=" << size << std::endl;

            len = get_read_int_length(buf);
            total_len += len;
            int64_t id = read_long(buf);
            buf += len;
            std::cout << "id = " << id << ", len=" << total_len << ", size=" << size << std::endl;

            len = get_read_int_length(buf);
            total_len += len;
            int64_t tm = read_long(buf);
            buf += len;
            std::cout << "tm = " << tm << ", len=" << total_len << ", size=" << size << std::endl;

            // repMasterNodeId
            len = get_read_int_length(buf);
            total_len += len;

            len = get_read_int_length(buf);
            total_len += len;
            std::cout << "total_len=" << total_len << ", size=" << size << std::endl;

            return true;
        } else if (entry_type == 33) {
            int total_len = 0;
            flags = *buf++;
            flags2 = *buf++;
            total_len += 2;
            std::cout << "flags=" << (uint32_t)flags << ", flags2=" << (uint32_t)flags2 << std::endl;

            // dbid
            int len = get_read_int_length(buf);
            int64_t dbid = read_long(buf);
            buf += len;
            total_len += len;
            std::cout << "dbid=" << dbid << std::endl;

            // Txn: id lastLoggedLsn
            len = get_read_int_length(buf);
            buf += len;
            total_len += len;
            len = get_read_int_length(buf);
            buf += len;
            total_len += len;

            // LN: size + data
            len = get_read_int_length(buf);
            int data_size = read_int(buf);
            total_len += len;
            buf += len;

            uint16_t op = read_short(buf);
            buf += data_size;
            
            int64_t key = read_uint64(buf);
            std::cout << "op=" << (uint32_t)op << ", key=" << key << ", left_size=" << size - total_len - data_size << std::endl;

            return true;
        } else {
            return true;
        }

        return true;
    }


public:
    uint8_t flags;
    uint8_t flags2;
    uint16_t op_code = 0;
};

class EntryReader {
public:
    bool read_next(int fd) {
        if (!read_uint32(fd, header.check_sum)) {
            return false;
        }
        if (!read_uint8(fd, header.entry_type)) {
            return false;
        }
        if (!read_uint8(fd, header.entry_flag)) {
            return false;
        }
        if (!read_uint32(fd, header.prev_offset)) {
            return false;
        }
        if (!read_uint32(fd, header.item_size)) {
            return false;
        }
        vlsn_flag = (header.entry_flag & 0x20) | (header.entry_flag & 0x08);
        if (vlsn_flag) {
            //std::cout << "need to read vlsn" << std::endl;
            if (!read_uint64(fd, header.vlsn)) {
                return false;
            }
        } else {
            header.vlsn = 0;
        }

        int8_t buf[header.item_size];
        if (read(fd, buf, header.item_size) != header.item_size) {
            std::cout << "read item failed" << std::endl;
            return false;
        }
        if (!item.set_data(buf, header.item_size, header.entry_type)) {
            std::cout << "set data for item failed" << std::endl;
            return false;
        }
        
        return true;
    }
    bool seek_next(int fd) {
        cur_offset = lseek(fd, 0, SEEK_CUR);
        return cur_offset >= 0;
    }
    bool read_uint64(int fd, uint64_t& ret) {
        uint8_t buf[sizeof(uint64_t)];
        if (read(fd, buf, sizeof(uint64_t)) != sizeof(uint64_t)) {
            return false;
        }

        ret = buf[0] + (buf[1] << 8) + (buf[2] << 16) + (buf[3] << 24)
                + ((uint64_t)buf[4] << 32) + ((uint64_t)buf[5] << 40) + ((uint64_t)buf[6] << 48) + ((uint64_t)buf[7] << 56);
        return true;
    }
    bool read_uint32(int fd, uint32_t& ret) {
        uint8_t buf[sizeof(uint32_t)];
        if (read(fd, buf, sizeof(uint32_t)) != sizeof(uint32_t)) {
            return false;
        }

        ret = buf[0] + (buf[1] << 8) + (buf[2] << 16) + (buf[3] << 24);
        return true;
    }
    bool read_uint16(int fd, uint16_t& ret) {
        uint8_t buf[sizeof(uint16_t)];
        if (read(fd, buf, sizeof(uint16_t)) != sizeof(uint16_t)) {
            return false;
        }

        ret = buf[1] + (buf[0] << 8);
        return true;
    }
    bool read_uint8(int fd, uint8_t& ret) {
        uint8_t buf[sizeof(uint8_t)];
        if (read(fd, buf, sizeof(uint8_t)) != sizeof(uint8_t)) {
            return false;
        }

        ret = buf[0];
        return true;
    }

    std::string to_string() {
        std::stringstream ss;
        ss << "cur_offset=" << cur_offset;
        ss << ", last_offset=" << header.prev_offset;
        ss << ", entry_type=" << (uint32_t)header.entry_type;
        ss << ", flag=" << (uint32_t)header.entry_flag;
        ss << ", item_size=" << header.item_size;
        ss << ", vlsn_flag=" << vlsn_flag;
        ss << ", vlsn=" << header.vlsn;
        //ss << ", journalid=" << item.journal_id;
        //ss << ", op_code=" << item.op_code;
        //ss << ", item_flags=" << (uint32_t)(item.flags | item.flags2);
        return ss.str();
    }

private:
    JDBLogHeader header;
    LogEntity item;
    int64_t cur_offset = 0;
    bool vlsn_flag = false;
};
*/

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cout << "Usage: cmd filename" << std::endl;
        return -1;
    }

    JDBFileParser parser;
    if (!parser.init(argv[1])) {
        return -1;
    }
    while (parser.read_next_log()) {
        parser.display();
    }
    return 0;

/*
    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) {
        std::cout << "open file failed:" << argv[1] << std::endl;
        return -1;
    }
    std::cout << "open file success:" << argv[1] << std::endl;

    EntryReader reader;
    while (reader.read_next(fd)) {
        std::cout << "Finish Read one entry: " << reader.to_string() << std::endl;
        if (!reader.seek_next(fd)) {
            std::cout << "seek failed" << std::endl;
        }
    }

    close(fd);
    return 0;
*/
}
