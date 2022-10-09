package org.sanofi.eimadaptor.service;

import org.sanofi.eimadaptor.vo.DqRuleJson;

public interface IDqRuleService {
    DqRuleJson getDqRuleJson(String fileName);
}
