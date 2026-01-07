;;; -*- lexical-binding: t -*-
(custom-set-variables
 ;; custom-set-variables was added by Custom.
 ;; If you edit it by hand, you could mess it up, so be careful.
 ;; Your init file should contain only one such instance.
 ;; If there is more than one, they won't work right.
 '(custom-safe-themes
   '("2f6ca293861c427ed84c7250fb2a60bcadede1a856bf69e1d275db6eee2249f5"
     default))
 '(package-selected-packages '(company flycheck gptel lsp-mode lsp-ui transient)))
(custom-set-faces
 ;; custom-set-faces was added by Custom.
 ;; If you edit it by hand, you could mess it up, so be careful.
 ;; Your init file should contain only one such instance.
 ;; If there is more than one, they won't work right.
 )

(load-theme 'wombat t)
(menu-bar-mode -1)
(tool-bar-mode -1)
(scroll-bar-mode -1)
(require 'recentf)
(recentf-mode 1)

(global-set-key (kbd "C-c o") #'recentf-open-files)

;;;; Package setup
(require 'package)
(setq package-archives
      '(("melpa" . "https://melpa.org/packages/")
        ("gnu"   . "https://elpa.gnu.org/packages/")))
(package-initialize)

(unless (package-installed-p 'use-package)
  (package-refresh-contents)
  (package-install 'use-package))
(require 'use-package)
(setq use-package-always-ensure t)

;;;; Basics: parenthesis pairing + line numbers
(electric-pair-mode 1)                    ;; auto-insert matching parens/brackets/quotes
;;(setq display-line-numbers-type 't) ;; or 't for absolute
(global-display-line-numbers-mode 1)

;; Optional: disable line numbers in some modes where they’re annoying
(dolist (mode '(term-mode eshell-mode shell-mode vterm-mode))
  (add-hook (intern (format "%s-hook" mode))
            (lambda () (display-line-numbers-mode 0))))
(require 'tramp)
;;;; Autocomplete framework
(use-package company
  :hook (after-init . global-company-mode)
  :config
  (setq company-idle-delay 0.1
        company-minimum-prefix-length 1
        company-tooltip-align-annotations t))

;;;; LSP (Language Server Protocol) for Python intelligence
(use-package lsp-mode
  :commands lsp
  :hook ((python-mode . lsp))
  :config
  (setq lsp-idle-delay 0.2
        lsp-log-io nil))

(use-package lsp-ui
  :after lsp-mode
  :hook (lsp-mode . lsp-ui-mode)
  :config
  (setq lsp-ui-doc-enable t
        lsp-ui-sideline-enable t))

;; Optional but nice: syntax checking
(use-package flycheck
  :hook (after-init . global-flycheck-mode))


(use-package gptel
  :ensure t
  :commands (gptel gptel-send gptel-menu)
  :bind (("C-c g" . gptel)
         ("C-c G" . gptel-send)))
(setq gptel-api-key (getenv "OPENAI_API_KEY"))
(setq gptel-default-mode 'org-mode)
(setq gptel-model "gpt-5.1")


(add-hook 'shell-mode-hook (lambda () (company-mode -1)))
(add-hook 'eshell-mode-hook (lambda () (company-mode -1)))


(use-package exec-path-from-shell
  :ensure t
  :config
  ;; Make Emacs inherit PATH + env vars from your zsh
  (exec-path-from-shell-initialize)
  (exec-path-from-shell-copy-env "OPENAI_API_KEY"))

(use-package aidermacs
  :ensure t
  :bind (("C-c a" . aidermacs-transient-menu))  ; open its main menu
  :config
  (setq aidermacs-aider-command "aider")        ; use the aider CLI
  ;; Optional: set default model (inherits from your env)
  (setq aidermacs-default-model "gpt-4.1"))

(setq iso-transl-char-map nil)
(global-set-key (kbd "M-p") #'scroll-down-command)
(global-set-key (kbd "M-n") #'scroll-up-command)

;; Save all auto-save files in ~/.emacs.d/auto-saved/
(defconst my-auto-save-folder (expand-file-name "~/.emacs.d/auto-saved/"))
(unless (file-exists-p my-auto-save-folder)
  (make-directory my-auto-save-folder t))
(setq auto-save-file-name-transforms
      `((".*" ,(concat my-auto-save-folder "\\1") t)))
