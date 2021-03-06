/* Copyright (C) 2016 NooBaa */

import template from './editor.html';

class EditorViewModel {
    constructor({
        label = '',
        visible = true,
        disabled = false,
        tooltip = '',
        insertValMessages = true
    }) {
        this.label = label;
        this.visible = visible;
        this.disabled = disabled;
        this.tooltip = tooltip;
        this.insertValMessages = insertValMessages;
    }
}

export default {
    viewModel: EditorViewModel,
    template: template
};
