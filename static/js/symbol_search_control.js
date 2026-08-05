(function () {
  class SymbolSearchControl {
    constructor({
      root,
      selectedSymbol,
      allowedSymbols,
      onSymbolChange,
      showQuickButtons = true,
      placeholder = "Search ticker",
      unsupportedMessage = "Only QQQ, SPY, and SPX are supported.",
      activeClass = "is-active",
      quickButtonSelector = "[data-symbol-quick]",
      popoverQuickButtonSelector = "[data-symbol-popover-quick]",
      toggleSelector = "[data-symbol-search-toggle]",
      popoverSelector = "[data-symbol-search-popover]",
      formSelector = "[data-symbol-search-form]",
      inputSelector = "[data-symbol-search-input]",
      messageSelector = "[data-symbol-search-message]",
    }) {
      this.root = root;
      this.selectedSymbol = this.sanitize(selectedSymbol);
      this.allowedSymbols = (Array.isArray(allowedSymbols) ? allowedSymbols : [])
        .map((symbol) => this.sanitize(symbol))
        .filter(Boolean);
      this.allowedSet = new Set(this.allowedSymbols);
      this.onSymbolChange = typeof onSymbolChange === "function" ? onSymbolChange : () => {};
      this.showQuickButtons = showQuickButtons;
      this.placeholder = placeholder;
      this.unsupportedMessage = unsupportedMessage;
      this.activeClass = activeClass;
      this.quickButtons = Array.from(root.querySelectorAll(quickButtonSelector));
      this.popoverQuickButtons = Array.from(root.querySelectorAll(popoverQuickButtonSelector));
      this.toggle = root.querySelector(toggleSelector);
      this.popover = root.querySelector(popoverSelector);
      this.form = root.querySelector(formSelector);
      this.input = root.querySelector(inputSelector);
      this.message = root.querySelector(messageSelector);
      this.handleDocumentClick = this.handleDocumentClick.bind(this);
      this.handleDocumentKeydown = this.handleDocumentKeydown.bind(this);
      this.init();
    }

    sanitize(value) {
      return String(value || "")
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9.-]/g, "")
        .slice(0, 12);
    }

    init() {
      if (this.input) {
        this.input.placeholder = this.placeholder;
        this.input.value = this.selectedSymbol;
        this.input.addEventListener("input", () => {
          const nextValue = this.sanitize(this.input.value);
          if (this.input.value !== nextValue) this.input.value = nextValue;
          this.setMessage("");
        });
      }
      if (this.toggle) {
        this.toggle.addEventListener("click", (event) => {
          event.preventDefault();
          event.stopPropagation();
          this.setOpen(this.popover ? this.popover.hidden : false);
        });
      }
      if (this.form) {
        this.form.addEventListener("submit", (event) => {
          event.preventDefault();
          this.submit(this.input ? this.input.value : "");
        });
      }
      this.quickButtons.forEach((button) => {
        button.addEventListener("click", (event) => {
          event.preventDefault();
          this.submit(button.dataset.symbolQuick || button.textContent);
        });
      });
      this.popoverQuickButtons.forEach((button) => {
        button.hidden = !this.showQuickButtons;
        button.addEventListener("click", (event) => {
          event.preventDefault();
          this.submit(button.dataset.symbolPopoverQuick || button.textContent);
        });
      });
      document.addEventListener("click", this.handleDocumentClick);
      document.addEventListener("keydown", this.handleDocumentKeydown);
      this.setSelected(this.selectedSymbol);
    }

    setMessage(message) {
      if (!this.message) return;
      this.message.textContent = message || "";
      this.message.hidden = !message;
    }

    setOpen(open) {
      if (!this.popover || !this.toggle) return;
      this.popover.hidden = !open;
      this.toggle.setAttribute("aria-expanded", open ? "true" : "false");
      this.root.classList.toggle("is-symbol-search-open", open);
      this.setMessage("");
      if (open && this.input) {
        window.setTimeout(() => {
          this.input.focus();
          this.input.select();
        }, 0);
      }
    }

    close() {
      this.setOpen(false);
    }

    handleDocumentClick(event) {
      if (!this.popover || this.popover.hidden) return;
      if (this.root.contains(event.target)) return;
      this.close();
    }

    handleDocumentKeydown(event) {
      if (event.key === "Escape") this.close();
    }

    setSelected(symbol) {
      const nextSymbol = this.sanitize(symbol);
      if (!nextSymbol) return;
      this.selectedSymbol = nextSymbol;
      if (this.input) this.input.value = nextSymbol;
      this.quickButtons.forEach((button) => {
        const buttonSymbol = this.sanitize(button.dataset.symbolQuick || button.textContent);
        const isActive = buttonSymbol === nextSymbol;
        button.classList.toggle(this.activeClass, isActive);
        button.classList.toggle("active", isActive);
        button.setAttribute("aria-selected", isActive ? "true" : "false");
      });
    }

    submit(value) {
      const nextSymbol = this.sanitize(value);
      if (!nextSymbol) return;
      if (this.allowedSet.size && !this.allowedSet.has(nextSymbol)) {
        this.setMessage(this.unsupportedMessage);
        return;
      }
      this.setSelected(nextSymbol);
      this.close();
      this.onSymbolChange(nextSymbol);
    }
  }

  window.SymbolSearchControl = SymbolSearchControl;
})();
