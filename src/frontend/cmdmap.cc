#include "cmdmap.h"
#include "errors/protocol.h"

std::error_code foxbatdb::Command::Validate() const {
    // 校验主命令参数数量是否正确
    auto& mainCmdWrapper = MainCommandMap.at(this->name);
    if ((this->argv.size() < mainCmdWrapper.minArgc) ||
        (this->argv.size() > mainCmdWrapper.maxArgc)) {
        return error::ProtocolErrorCode::kArgNumbers;
    }
    // 校验主命令选项参数
    auto& opts = this->options;
    for (const auto& opt: opts) {
        auto& cmdOptWrapper = CommandOptionMap.at(opt.name);
        // 校验主命令选项参数是否存在互斥
        for (auto optType: cmdOptWrapper.exclusiveOpts) {
            if (opts.end() != std::find_if(opts.begin(), opts.end(),
                                           [optType](const CommandOption& o) { return o.type == optType; })) {
                return error::ProtocolErrorCode::kOptionExclusive;
            }
        }
        // 校验主命令选项参数数量是否正确
        if ((opt.argv.size() < cmdOptWrapper.minArgc) ||
            (opt.argv.size() > cmdOptWrapper.maxArgc)) {
            return error::ProtocolErrorCode::kArgNumbers;
        }
    }
    return error::ProtocolErrorCode::kSuccess;
}
