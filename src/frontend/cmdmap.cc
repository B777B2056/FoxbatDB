#include "cmdmap.h"
#include "errors/protocol.h"

std::error_code foxbatdb::Command::Validate() const {
    // У����������������Ƿ���ȷ
    auto& mainCmdWrapper = MainCommandMap.at(this->name);
    if ((this->argv.size() < mainCmdWrapper.minArgc) ||
        (this->argv.size() > mainCmdWrapper.maxArgc)) {
        return error::ProtocolErrorCode::kArgNumbers;
    }
    // У��������ѡ�����
    auto& opts = this->options;
    for (const auto& opt: opts) {
        auto& cmdOptWrapper = CommandOptionMap.at(opt.name);
        // У��������ѡ������Ƿ���ڻ���
        for (auto optType: cmdOptWrapper.exclusiveOpts) {
            if (opts.end() != std::find_if(opts.begin(), opts.end(),
                                           [optType](const CommandOption& o) { return o.type == optType; })) {
                return error::ProtocolErrorCode::kOptionExclusive;
            }
        }
        // У��������ѡ����������Ƿ���ȷ
        if ((opt.argv.size() < cmdOptWrapper.minArgc) ||
            (opt.argv.size() > cmdOptWrapper.maxArgc)) {
            return error::ProtocolErrorCode::kArgNumbers;
        }
    }
    return error::ProtocolErrorCode::kSuccess;
}
