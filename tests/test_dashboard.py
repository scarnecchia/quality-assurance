from __future__ import annotations

import pytest
import jinja2

from scdm_qa.reporting.dashboard import (
    _load_vendor_asset,
    _get_template_env,
    _render_page,
    save_dashboard,
)


class TestLoadVendorAsset:
    def test_loads_known_vendor_file(self) -> None:
        """Test that a known vendor file can be loaded."""
        content = _load_vendor_asset("tabulator.min.js")
        assert content, "loaded file should not be empty"
        assert isinstance(content, str)

    def test_loaded_content_is_non_empty(self) -> None:
        """Test that loaded files contain actual content."""
        for filename in ("tabulator.min.js", "tabulator.min.css", "plotly-basic.min.js"):
            content = _load_vendor_asset(filename)
            assert len(content) > 100, f"{filename} should contain substantial content"

    def test_raises_on_nonexistent_file(self) -> None:
        """Test that loading a nonexistent file raises an error."""
        with pytest.raises(FileNotFoundError):
            _load_vendor_asset("nonexistent-file.js")

    def test_rejects_path_traversal_with_forward_slash(self) -> None:
        """Test that filenames with forward slashes are rejected."""
        with pytest.raises(ValueError, match="invalid vendor asset filename"):
            _load_vendor_asset("../../config.py")

    def test_rejects_path_traversal_with_backslash(self) -> None:
        """Test that filenames with backslashes are rejected."""
        with pytest.raises(ValueError, match="invalid vendor asset filename"):
            _load_vendor_asset("..\\..\\config.py")

    def test_rejects_single_forward_slash(self) -> None:
        """Test that any forward slash in filename is rejected."""
        with pytest.raises(ValueError, match="invalid vendor asset filename"):
            _load_vendor_asset("subdir/file.js")

    def test_rejects_single_backslash(self) -> None:
        """Test that any backslash in filename is rejected."""
        with pytest.raises(ValueError, match="invalid vendor asset filename"):
            _load_vendor_asset("subdir\\file.js")


class TestGetTemplateEnv:
    def test_returns_jinja2_environment(self) -> None:
        """Test that _get_template_env returns a Jinja2 Environment."""
        env = _get_template_env()
        assert isinstance(env, jinja2.Environment)

    def test_can_load_base_html_template(self) -> None:
        """Test that the base.html template can be loaded from the environment."""
        env = _get_template_env()
        template = env.get_template("base.html")
        assert template is not None
        assert hasattr(template, "render")

    def test_environment_has_autoescape_enabled(self) -> None:
        """Test that autoescape is configured."""
        env = _get_template_env()
        assert env.autoescape is True

    def test_environment_uses_package_loader(self) -> None:
        """Test that the environment uses PackageLoader."""
        env = _get_template_env()
        assert isinstance(env.loader, jinja2.PackageLoader)


class TestRenderPage:
    def test_renders_base_html_with_page_title(self) -> None:
        """Test that base.html can be rendered with page_title context."""
        html = _render_page("base.html", page_title="Test Page")
        assert html is not None
        assert isinstance(html, str)
        assert len(html) > 0

    def test_rendered_output_contains_doctype(self) -> None:
        """Test that rendered HTML starts with DOCTYPE."""
        html = _render_page("base.html", page_title="Test")
        assert "<!DOCTYPE html>" in html

    def test_rendered_output_contains_tabulator_content(self) -> None:
        """Test that Tabulator JS and CSS are inlined in the output."""
        html = _render_page("base.html", page_title="Test")
        # Check for Tabulator JS signature
        assert "Tabulator" in html or "tabulator" in html.lower()
        # Check for Tabulator CSS signature (should have tabulator-specific rules)
        assert ".tabulator" in html or "tabulator" in html.lower()

    def test_rendered_output_contains_plotly_content(self) -> None:
        """Test that Plotly JS is inlined in the output."""
        html = _render_page("base.html", page_title="Test")
        # Check for Plotly JS signature
        assert "Plotly" in html or "plotly" in html.lower()

    def test_rendered_output_contains_page_title_in_html_title(self) -> None:
        """Test that the page_title appears in the HTML title tag."""
        html = _render_page("base.html", page_title="Dashboard Report")
        assert "Dashboard Report" in html
        assert "<title>" in html

    def test_rendered_output_contains_dashboard_text_in_title(self) -> None:
        """Test that 'SCDM-QA Dashboard' appears in the title."""
        html = _render_page("base.html", page_title="Test")
        assert "SCDM-QA Dashboard" in html

    def test_custom_context_variables_are_passed_to_template(self) -> None:
        """Test that additional context variables are passed to the template."""
        # The base.html template doesn't use custom vars, but we test that
        # the render function accepts and passes them through
        html = _render_page("base.html", page_title="Test", custom_var="custom_value")
        # Should render without error
        assert "<!DOCTYPE html>" in html

    def test_renders_with_multiple_context_variables(self) -> None:
        """Test that multiple context variables can be passed."""
        html = _render_page(
            "base.html",
            page_title="Test",
            var1="value1",
            var2="value2",
        )
        assert "<!DOCTYPE html>" in html


class TestSaveDashboard:
    def test_save_dashboard_raises_not_implemented(self) -> None:
        """Test that save_dashboard raises NotImplementedError."""
        from pathlib import Path

        with pytest.raises(NotImplementedError):
            save_dashboard(Path("/tmp"), [])

    def test_save_dashboard_message_indicates_not_yet_implemented(self) -> None:
        """Test that the NotImplementedError message is clear."""
        from pathlib import Path

        with pytest.raises(NotImplementedError, match="not yet implemented"):
            save_dashboard(Path("/tmp"), [])
