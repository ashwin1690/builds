"""
Data Stories Narratives Extractor

Extracts Data Stories and their narrative content from Tableau workbooks.
"""

from typing import Optional
from lxml import etree

from ..schema import DataStory, StoryPoint


class StoryExtractor:
    """
    Extracts Data Stories from Tableau XML.

    Data Stories are sequential narratives that guide viewers through
    a series of insights using worksheets and dashboards.
    """

    def extract(self, story_element) -> Optional[DataStory]:
        """
        Extract a complete Data Story from a story element.

        Args:
            story_element: lxml Element representing a story

        Returns:
            DataStory object or None
        """
        story_name = story_element.get('name')
        if not story_name:
            return None

        # Generate story ID
        story_id = story_name.replace(' ', '_')

        # Create story
        story = DataStory(
            story_name=story_name,
            story_id=story_id
        )

        # Extract description if available
        description = story_element.get('description')
        if description:
            story.description = description

        # Extract story points
        story_points = self._extract_story_points(story_element)
        story.points = story_points

        return story

    def _extract_story_points(self, story_element) -> list:
        """
        Extract all story points from a story.

        Args:
            story_element: lxml Element representing a story

        Returns:
            List of StoryPoint objects
        """
        points = []

        # Find story points container
        story_points_elem = story_element.find('.//story-points')
        if story_points_elem is None:
            return points

        # Process each story point
        order = 0
        for point_elem in story_points_elem.findall('story-point'):
            point = self._extract_story_point(point_elem, order)
            if point:
                points.append(point)
                order += 1

        return points

    def _extract_story_point(self, point_elem, order: int) -> Optional[StoryPoint]:
        """
        Extract a single story point.

        Args:
            point_elem: lxml Element representing a story point
            order: Order index of the story point

        Returns:
            StoryPoint object or None
        """
        # Get caption (required)
        caption = point_elem.get('caption')
        if not caption:
            caption = f"Story Point {order + 1}"

        # Generate point ID
        point_id = f"point_{order}"

        # Create story point
        point = StoryPoint(
            point_id=point_id,
            caption=caption,
            order=order
        )

        # Extract description/narrative text
        description = point_elem.get('description')
        if description:
            point.description = description

        # Extract narrative text annotation
        narrative_text = self._extract_narrative_text(point_elem)
        if narrative_text:
            point.narrative_text = narrative_text

        # Extract referenced worksheet or dashboard
        worksheet_name = self._extract_worksheet_reference(point_elem)
        if worksheet_name:
            point.worksheet_name = worksheet_name

        dashboard_name = self._extract_dashboard_reference(point_elem)
        if dashboard_name:
            point.dashboard_name = dashboard_name

        return point

    def _extract_narrative_text(self, point_elem) -> Optional[str]:
        """
        Extract narrative text annotations from a story point.

        Narrative text includes text boxes and annotations added to story points.
        """
        narratives = []

        # Look for zone elements with text/annotations
        for zone in point_elem.findall('.//zone'):
            zone_type = zone.get('type')

            # Look for text zones
            if zone_type == 'text':
                text_elem = zone.find('.//text')
                if text_elem is not None:
                    text_content = text_elem.text
                    if text_content:
                        narratives.append(text_content.strip())

            # Look for formatted text
            formatted_text = zone.find('.//formatted-text')
            if formatted_text is not None:
                # Extract text from run elements
                for run in formatted_text.findall('.//run'):
                    run_text = run.text
                    if run_text:
                        narratives.append(run_text.strip())

        # Also check for caption annotations
        for annotation in point_elem.findall('.//annotation'):
            annotation_text = annotation.get('text')
            if annotation_text:
                narratives.append(annotation_text.strip())

        # Combine all narrative texts
        if narratives:
            return '\n\n'.join(narratives)

        return None

    def _extract_worksheet_reference(self, point_elem) -> Optional[str]:
        """Extract worksheet reference from a story point"""
        # Look for worksheet element
        worksheet = point_elem.find('.//worksheet')
        if worksheet is not None:
            return worksheet.get('name')

        # Also check zone references
        for zone in point_elem.findall('.//zone'):
            worksheet_name = zone.get('worksheet')
            if worksheet_name:
                return worksheet_name

        return None

    def _extract_dashboard_reference(self, point_elem) -> Optional[str]:
        """Extract dashboard reference from a story point"""
        # Look for dashboard element
        dashboard = point_elem.find('.//dashboard')
        if dashboard is not None:
            return dashboard.get('name')

        # Also check zone references
        for zone in point_elem.findall('.//zone'):
            dashboard_name = zone.get('dashboard')
            if dashboard_name:
                return dashboard_name

        return None
