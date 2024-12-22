from setuptools import setup, find_packages

setup(
    name='botnim',
    version='0.1.0',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'botnim=botnim.cli:main',
        ],
    },
    install_requires=[
        'dataflows',
        'dataflows-airtable',
        'python-dotenv',
        'openai',
        'pyyaml',
        'requests',
        'requests-openapi',
        'click',
        'html2text',
        'bs4',
        'gspread',
        'oauth2client'
    ],
    author='While True Industries',
    author_email='adam@whiletrue.industries',
    description='Botnim Specs and Code',
    python_requires='>=3.10',
)
